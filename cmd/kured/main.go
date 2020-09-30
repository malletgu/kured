package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/weaveworks/kured/pkg/alerts"
	"github.com/weaveworks/kured/pkg/cloud"
	"github.com/weaveworks/kured/pkg/daemonsetlock"
	"github.com/weaveworks/kured/pkg/delaytick"
	"github.com/weaveworks/kured/pkg/notifications/slack"
	"github.com/weaveworks/kured/pkg/timewindow"
)

var (
	version = "unreleased"

	// Command line flags
	period            time.Duration
	dsNamespace       string
	dsName            string
	lockAnnotation    string
	prometheusURL     string
	alertFilter       *regexp.Regexp
	rebootSentinel    string
	rebootCommand     string
	terminateSentinel string
	terminateCommand  string
	slackHookURL      string
	slackUsername     string
	slackChannel      string
	podSelectors      []string

	awsTargetGroupArn    string

	rebootDays  []string
	rebootStart string
	rebootEnd   string
	timezone    string

	annotationTTL time.Duration

	// Metrics
	rebootRequiredGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "kured",
		Name:      "reboot_required",
		Help:      "OS requires reboot due to software updates.",
	}, []string{"node"})
)

func init() {
	prometheus.MustRegister(rebootRequiredGauge)
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "kured",
		Short: "Kubernetes Reboot Daemon",
		Run:   root}

	rootCmd.PersistentFlags().DurationVar(&period, "period", time.Minute*60,
		"reboot check period")
	rootCmd.PersistentFlags().StringVar(&dsNamespace, "ds-namespace", "kube-system",
		"namespace containing daemonset on which to place lock")
	rootCmd.PersistentFlags().StringVar(&dsName, "ds-name", "kured",
		"name of daemonset on which to place lock")
	rootCmd.PersistentFlags().StringVar(&lockAnnotation, "lock-annotation", "weave.works/kured-node-lock",
		"annotation in which to record locking node")
	rootCmd.PersistentFlags().StringVar(&prometheusURL, "prometheus-url", "",
		"Prometheus instance to probe for active alerts")
	rootCmd.PersistentFlags().Var(&regexpValue{&alertFilter}, "alert-filter-regexp",
		"alert names to ignore when checking for active alerts")

	rootCmd.PersistentFlags().StringVar(&rebootSentinel, "reboot-sentinel", "/var/run/reboot-required",
		"path to file whose existence signals need to reboot")
	rootCmd.PersistentFlags().StringVar(&rebootCommand, "reboot-command", "reboot",
		"command to execute for the reboot")

	rootCmd.PersistentFlags().StringVar(&terminateSentinel, "terminate-sentinel", "/var/run/terminate-required",
		"path to file whose existence signals need to terminate")
	rootCmd.PersistentFlags().StringVar(&terminateCommand, "terminate-command", "shutdown",
		"command to execute for the terminate, note that the lock will be released prior to this instance being terminated." +
		"This functionality requires a valid service account with the right to terminate instances")

	rootCmd.PersistentFlags().StringVar(&awsTargetGroupArn, "target-group-arn", "",
		"enable removing the instance from aws target group prior to restart and re-adding it after." +
		"This functionality requires a valid service account with the right to edit that target group")

	rootCmd.PersistentFlags().StringVar(&slackHookURL, "slack-hook-url", "",
		"slack hook URL for reboot notfications")
	rootCmd.PersistentFlags().StringVar(&slackUsername, "slack-username", "kured",
		"slack username for reboot notfications")
	rootCmd.PersistentFlags().StringVar(&slackChannel, "slack-channel", "",
		"slack channel for reboot notfications")

	rootCmd.PersistentFlags().StringArrayVar(&podSelectors, "blocking-pod-selector", nil,
		"label selector identifying pods whose presence should prevent reboots")

	rootCmd.PersistentFlags().StringSliceVar(&rebootDays, "reboot-days", timewindow.EveryDay,
		"schedule reboot on these days")
	rootCmd.PersistentFlags().StringVar(&rebootStart, "start-time", "0:00",
		"schedule reboot only after this time of day")
	rootCmd.PersistentFlags().StringVar(&rebootEnd, "end-time", "23:59:59",
		"schedule reboot only before this time of day")
	rootCmd.PersistentFlags().StringVar(&timezone, "time-zone", "UTC",
		"use this timezone for schedule inputs")

	rootCmd.PersistentFlags().DurationVar(&annotationTTL, "annotation-ttl", 0,
		"force clean annotation after this ammount of time (default 0, disabled)")

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

// newCommand creates a new Command with stdout/stderr wired to our standard logger
func newCommand(executeAsHost bool, name string, args ...string) *exec.Cmd {
	var cmd *exec.Cmd
	if executeAsHost {
		cmd = exec.Command("/usr/bin/nsenter", append([]string{"-m/proc/1/ns/mnt", name}, args...)...)
	} else {
		cmd = exec.Command(name, args...)
	}

	cmd.Stdout = log.NewEntry(log.StandardLogger()).
		WithField("cmd", cmd.Args[0]).
		WithField("std", "out").
		WriterLevel(log.InfoLevel)

	cmd.Stderr = log.NewEntry(log.StandardLogger()).
		WithField("cmd", cmd.Args[0]).
		WithField("std", "err").
		WriterLevel(log.WarnLevel)

	return cmd
}

func sentinelExists(sentinel string) bool {
	// Relies on hostPID:true and privileged:true to enter host mount space
	sentinelCmd := newCommand(true, "--", "/usr/bin/test", "-f", sentinel)
	if err := sentinelCmd.Run(); err != nil {
		switch err := err.(type) {
		case *exec.ExitError:
			// We assume a non-zero exit code means 'reboot not required', but of course
			// the user could have misconfigured the sentinel command or something else
			// went wrong during its execution. In that case, not entering a reboot loop
			// is the right thing to do, and we are logging stdout/stderr of the command
			// so it should be obvious what is wrong.
			return false
		default:
			// Something was grossly misconfigured, such as the command path being wrong.
			log.Fatalf("Error invoking sentinel command: %v", err)
		}
	}
	return true
}

func rebootRequired() bool {
	if sentinelExists(rebootSentinel) {
		log.Infof("Reboot required")
		return true
	} else {
		log.Infof("Reboot not required")
		return false
	}
}

func terminateRequired() bool {
	if sentinelExists(terminateSentinel) {
		log.Infof("Terminate required")
		return true
	} else {
		log.Infof("Terminate not required")
		return false
	}
}

func rebootBlocked(client *kubernetes.Clientset, nodeID string) bool {
	if prometheusURL != "" {
		alertNames, err := alerts.PrometheusActiveAlerts(prometheusURL, alertFilter)
		if err != nil {
			log.Warnf("Reboot blocked: prometheus query error: %v", err)
			return true
		}
		count := len(alertNames)
		if count > 10 {
			alertNames = append(alertNames[:10], "...")
		}
		if count > 0 {
			log.Warnf("Reboot blocked: %d active alerts: %v", count, alertNames)
			return true
		}
	}

	fieldSelector := fmt.Sprintf("spec.nodeName=%s", nodeID)
	for _, labelSelector := range podSelectors {
		podList, err := client.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{
			LabelSelector: labelSelector,
			FieldSelector: fieldSelector,
			Limit:         10})
		if err != nil {
			log.Warnf("Reboot blocked: pod query error: %v", err)
			return true
		}

		if len(podList.Items) > 0 {
			podNames := make([]string, 0, len(podList.Items))
			for _, pod := range podList.Items {
				podNames = append(podNames, pod.Name)
			}
			if len(podList.Continue) > 0 {
				podNames = append(podNames, "...")
			}
			log.Warnf("Reboot blocked: matching pods: %v", podNames)
			return true
		}
	}

	return false
}

func holding(lock *daemonsetlock.DaemonSetLock, metadata interface{}) bool {
	holding, err := lock.Test(metadata)
	if err != nil {
		log.Fatalf("Error testing lock: %v", err)
	}
	if holding {
		log.Infof("Holding lock")
	}
	return holding
}

func acquire(lock *daemonsetlock.DaemonSetLock, metadata interface{}, TTL time.Duration) bool {
	holding, holder, err := lock.Acquire(metadata, TTL)
	switch {
	case err != nil:
		log.Fatalf("Error acquiring lock: %v", err)
		return false
	case !holding:
		log.Warnf("Lock already held: %v", holder)
		return false
	default:
		log.Infof("Acquired reboot lock")
		return true
	}
}

func release(lock *daemonsetlock.DaemonSetLock) {
	log.Infof("Releasing lock")
	if err := lock.Release(); err != nil {
		log.Fatalf("Error releasing lock: %v", err)
	}
}

func drain(nodeID string) {
	log.Infof("Draining node %s", nodeID)

	if slackHookURL != "" {
		if err := slack.NotifyDrain(slackHookURL, slackUsername, slackChannel, nodeID); err != nil {
			log.Warnf("Error notifying slack: %v", err)
		}
	}

	drainCmd := newCommand(false, "/usr/bin/kubectl", "drain",
		"--ignore-daemonsets", "--delete-local-data", "--force", nodeID)

	if err := drainCmd.Run(); err != nil {
		log.Fatalf("Error invoking drain command: %v", err)
	}
}

func uncordon(nodeID string) {
	log.Infof("Uncordoning node %s", nodeID)
	uncordonCmd := newCommand(false, "/usr/bin/kubectl", "uncordon", nodeID)
	if err := uncordonCmd.Run(); err != nil {
		log.Fatalf("Error invoking uncordon command: %v", err)
	}
}

func executeCommand(nodeID string, action Action, command string) {
	log.Infof("Node %s Commanding %s: %s", nodeID, action, command)

	if slackHookURL != "" {
		if err := slack.NotifyReboot(slackHookURL, slackUsername, slackChannel, nodeID); err != nil {
			log.Warnf("Error notifying slack: %v", err)
		}
	}

	switch action {
	case Terminate:
		instanceId, err := cloud.GetInstanceIdFromNodeName(nodeID)
		if err != nil {
			log.Warnf("Cannot execute termination due to error finding the aws instance id")
		}

		// Terminate itself, commands aren't reliable past this point
		cloud.TerminateInstance(instanceId)
	case Restart:
		// Relies on hostPID:true and privileged:true to enter host mount space
		// Could be changed to try to use AWS session, this would reduce the amount of privilege for the pod
		// as we could as well mount the flags as a read only folder
		rebootCmd := newCommand(true, "/usr/bin/nsenter", "-m/proc/1/ns/mnt", "/bin/systemctl", command)
		if err := rebootCmd.Run(); err != nil {
			log.Fatalf("Node %v, error invoking %s command %s: %v", nodeID, action, command, err)
		}
	}
}

func maintainRebootRequiredMetric(nodeID string) {
	for {
		if terminateRequired() {
			rebootRequiredGauge.WithLabelValues(nodeID).Set(2)
		} else if rebootRequired() {
			rebootRequiredGauge.WithLabelValues(nodeID).Set(1)
		} else {
			rebootRequiredGauge.WithLabelValues(nodeID).Set(0)
		}
		time.Sleep(time.Minute)
	}
}

// nodeMeta is used to remember information across reboots
type nodeMeta struct {
	Unschedulable bool `json:"unschedulable"`
}

type Action string

const (
	Restart   Action = "restart"
	Terminate Action = "terminate"
	None      Action = "none"
)

func rebootAsRequired(nodeID string, window *timewindow.TimeWindow, TTL time.Duration) {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	lock := daemonsetlock.New(client, nodeID, dsNamespace, dsName, lockAnnotation)

	nodeMeta := nodeMeta{}
	if holding(lock, &nodeMeta) {
		if !nodeMeta.Unschedulable {
			uncordon(nodeID)
		}
		release(lock)

		if awsTargetGroupArn != "" {
			instanceId, err := cloud.GetInstanceIdFromNodeName(nodeID)
			if err != nil {
				log.Warnf("Cannot execute termination due to error finding the aws instance id")
			}
			cloud.RegisterTarget(*instanceId, awsTargetGroupArn)
		}
	}

	source := rand.NewSource(time.Now().UnixNano())
	tick := delaytick.New(source, period)
	for _ = range tick {

		var action = None
		if rebootRequired() {
			action = Restart
		} else if terminateRequired() {
			action = Terminate
		}

		if window.Contains(time.Now()) && action != None && !rebootBlocked(client, nodeID) {
			node, err := client.CoreV1().Nodes().Get(context.TODO(), nodeID, metav1.GetOptions{})
			if err != nil {
				log.Fatal(err)
			}
			nodeMeta.Unschedulable = node.Spec.Unschedulable

			if acquire(lock, &nodeMeta, TTL) {
				if !nodeMeta.Unschedulable {
					drain(nodeID)
				}
				if awsTargetGroupArn != "" {
					instanceId, err := cloud.GetInstanceIdFromNodeName(nodeID)
					if err != nil {
						log.Warnf("Cannot execute termination due to error finding the aws instance id")
					}
					cloud.DeregisterTarget(*instanceId, awsTargetGroupArn)
					time.Sleep(60 * time.Second)
				}

				switch action {
				case Restart:
					executeCommand(nodeID, action, rebootCommand)
				case Terminate:
					release(lock)
					executeCommand(nodeID, action, terminateCommand)
				}
				for {
					log.Infof("Waiting for reboot")
					time.Sleep(time.Minute)
				}
			}
		}
	}
}

func root(cmd *cobra.Command, args []string) {
	log.Infof("Kubernetes Reboot Daemon: %s", version)

	nodeID := os.Getenv("KURED_NODE_ID")
	if nodeID == "" {
		log.Fatal("KURED_NODE_ID environment variable required")
	}

	window, err := timewindow.New(rebootDays, rebootStart, rebootEnd, timezone)
	if err != nil {
		log.Fatalf("Failed to build time window: %v", err)
	}

	log.Infof("Node ID: %s", nodeID)
	log.Infof("Lock Annotation: %s/%s:%s", dsNamespace, dsName, lockAnnotation)
	log.Infof("Reboot Sentinel: %s every %v", rebootSentinel, period)
	log.Infof("Blocking Pod Selectors: %v", podSelectors)
	log.Infof("Reboot on: %v", window)
	if annotationTTL > 0 {
		log.Infof("Force annotation cleanup after: %v", annotationTTL)
	} else {
		log.Info("Force annotation cleanup disabled.")
	}

	go rebootAsRequired(nodeID, window, annotationTTL)
	go maintainRebootRequiredMetric(nodeID)

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":8080", nil))
}
