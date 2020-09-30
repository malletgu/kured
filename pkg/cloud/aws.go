package cloud

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/elbv2"

	log "github.com/sirupsen/logrus"

)

func GetInstanceIdFromNodeName(nodeID string) (*string, error) {
	awsSession, err := session.NewSession()
	if err != nil {
		log.Warnf("Error while trying to create aws session, skipping all steps involving AWS")
		return nil, err
	}
	svc := ec2.New(awsSession)
	input := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name: aws.String("private-dns-name"),
				Values: []*string{
					aws.String(nodeID),
				},
			},
		},
	}
	instances, err := svc.DescribeInstances(input)
	if err != nil {
		if awsError, ok := err.(awserr.Error); ok {
			switch awsError.Code() {
			default:
				log.Warnf(awsError.Error())
			}
		} else {
			log.Warnf(err.Error())
		}
		return nil, err
	}

	if len(instances.Reservations) != 1 || len(instances.Reservations[0].Instances) != 1 {
		return nil, errors.New("not exactly one instance has been found for this node id")
	}
	return instances.Reservations[0].Instances[0].InstanceId, nil
}

func TerminateInstance(instanceId *string) error {
	awsSession, err := session.NewSession()
	if err != nil {
		log.Warnf("Error while trying to create aws session, skipping all steps involving AWS")
		return err
	}
	svc := ec2.New(awsSession)
	input := &ec2.TerminateInstancesInput{
		InstanceIds: []*string{instanceId},
	}
	_, err = svc.TerminateInstances(input)
	return err
}

func DeregisterTarget(instanceId string, targetGroupArn string) error {
	awsSession, err := session.NewSession()
	if err != nil {
		log.Warnf("Error while trying to create aws session, skipping all steps involving AWS")
		return err
	}
	svc := elbv2.New(awsSession)

	input := &elbv2.DeregisterTargetsInput{
		TargetGroupArn: aws.String(targetGroupArn),
		Targets: []*elbv2.TargetDescription{
			{
				Id: aws.String(instanceId),
			},
		},
	}

	result, err := svc.DeregisterTargets(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case elbv2.ErrCodeTargetGroupNotFoundException:
				fmt.Println(elbv2.ErrCodeTargetGroupNotFoundException, aerr.Error())
			case elbv2.ErrCodeInvalidTargetException:
				fmt.Println(elbv2.ErrCodeInvalidTargetException, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
	}

	wait_input := &elbv2.DescribeTargetHealthInput{
		TargetGroupArn: &targetGroupArn,
		Targets: []*elbv2.TargetDescription{
			{
				Id: &instanceId,
			},
		},
	}
	fmt.Println(result)
	log.Info("Waiting for target deregistration")

	err = svc.WaitUntilTargetDeregistered(wait_input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case elbv2.ErrCodeTargetGroupNotFoundException:
				fmt.Println(elbv2.ErrCodeTargetGroupNotFoundException, aerr.Error())
			case elbv2.ErrCodeInvalidTargetException:
				fmt.Println(elbv2.ErrCodeInvalidTargetException, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
	} else {
		log.Info("Target deregistration complete")
	}

	return nil
}

func RegisterTarget(instanceId string, targetGroupArn string) error {
	awsSession, err := session.NewSession()
	if err != nil {
		log.Warnf("Error while trying to create aws session, skipping all steps involving AWS")
		return err
	}
	svc := elbv2.New(awsSession)

	input := &elbv2.RegisterTargetsInput{
		TargetGroupArn: aws.String(targetGroupArn),
		Targets: []*elbv2.TargetDescription{
			{
				Id: aws.String(instanceId),
			},
		},
	}

	result, err := svc.RegisterTargets(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case elbv2.ErrCodeTargetGroupNotFoundException:
				fmt.Println(elbv2.ErrCodeTargetGroupNotFoundException, aerr.Error())
			case elbv2.ErrCodeInvalidTargetException:
				fmt.Println(elbv2.ErrCodeInvalidTargetException, aerr.Error())
			case elbv2.ErrCodeTooManyRegistrationsForTargetIdException:
				fmt.Println(elbv2.ErrCodeTooManyRegistrationsForTargetIdException, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
	}

	fmt.Println(result)
	return nil
}