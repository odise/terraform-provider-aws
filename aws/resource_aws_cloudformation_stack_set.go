package aws

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudformation"
	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/helper/schema"
)

func resourceAwsCloudFormationStackSet() *schema.Resource {
	return &schema.Resource{
		Create: resourceAwsCloudFormationStackSetCreate,
		Read:   resourceAwsCloudFormationStackSetRead,
		Update: resourceAwsCloudFormationStackSetUpdate,
		Delete: resourceAwsCloudFormationStackSetDelete,

		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(30 * time.Minute),
			Update: schema.DefaultTimeout(30 * time.Minute),
			Delete: schema.DefaultTimeout(30 * time.Minute),
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"template_body": {
				Type:         schema.TypeString,
				Optional:     true,
				Computed:     true,
				ValidateFunc: validateCloudFormationTemplate,
				StateFunc: func(v interface{}) string {
					template, _ := normalizeCloudFormationTemplate(v)
					return template
				},
			},
			"template_url": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"capabilities": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Set:      schema.HashString,
			},
			"on_failure": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"parameters": {
				Type:     schema.TypeMap,
				Optional: true,
				Computed: true,
			},
			"tags": {
				Type:     schema.TypeMap,
				Optional: true,
			},
		},
	}
}

func resourceAwsCloudFormationStackSetCreate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).cfconn

	input := cloudformation.CreateStackSetInput{
		StackSetName: aws.String(d.Get("name").(string)),
	}
	if v, ok := d.GetOk("template_body"); ok {
		template, err := normalizeCloudFormationTemplate(v)
		if err != nil {
			return errwrap.Wrapf("template body contains an invalid JSON or YAML: {{err}}", err)
		}
		input.TemplateBody = aws.String(template)
	}
	if v, ok := d.GetOk("description"); ok {
		input.Description = aws.String(v.(string))
	}
	if v, ok := d.GetOk("template_url"); ok {
		input.TemplateURL = aws.String(v.(string))
	}
	if v, ok := d.GetOk("capabilities"); ok {
		input.Capabilities = expandStringList(v.(*schema.Set).List())
	}

	if v, ok := d.GetOk("parameters"); ok {
		input.Parameters = expandCloudFormationParameters(v.(map[string]interface{}))
	}

	if v, ok := d.GetOk("tags"); ok {
		input.Tags = expandCloudFormationTags(v.(map[string]interface{}))
	}

	log.Printf("[DEBUG] Creating CloudFormation Stack: %s", input)
	resp, err := conn.CreateStackSet(&input)
	if err != nil {
		return fmt.Errorf("Creating CloudFormation stack set failed: %s", err.Error())
	}

	d.SetId(*resp.StackSetId)
	var lastStatus string

	wait := resource.StateChangeConf{
		Pending: []string{
			"RUNNING",
			"STOPPING",
		},
		Target: []string{
			"DELETED",
			"ACTIVE",
		},
		Timeout:    d.Timeout(schema.TimeoutCreate),
		MinTimeout: 1 * time.Second,
		Refresh: func() (interface{}, string, error) {
			// XXX: change to https://docs.aws.amazon.com/sdk-for-go/api/service/cloudformation/#CloudFormation.DescribeStackSetOperation ?
			resp, err := conn.DescribeStackSet(&cloudformation.DescribeStackSetInput{
				StackSetName: aws.String(d.Id()),
			})
			if err != nil {
				// * ErrCodeStackSetNotFoundException "StackSetNotFoundException"
				// The specified stack set doesn't exist.
				// * ErrCodeOperationNotFoundException "OperationNotFoundException"
				// The specified ID refers to an operation that doesn't exist.
				//
				log.Printf("[ERROR] Failed to describe stacks: %s", err)
				return nil, "", err
			}

			status := *resp.StackSet.Status
			lastStatus = status
			log.Printf("[DEBUG] Current CloudFormation stack status: %q", status)

			return resp, status, err
		},
	}

	_, err = wait.WaitForState()
	if err != nil {
		return err
	}

	if lastStatus == "DELETED" {
		d.SetId("")

		return fmt.Errorf("%s: Stack set %q hast status %s.", d.Id(), lastStatus)
	}

	log.Printf("[INFO] CloudFormation Stack %q created", d.Id())

	return resourceAwsCloudFormationStackSetRead(d, meta)
}

func resourceAwsCloudFormationStackSetRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).cfconn

	input := &cloudformation.DescribeStackSetInput{
		StackSetName: aws.String(d.Id()),
	}
	resp, err := conn.DescribeStackSet(input)
	// XXX: delete handling
	/*

		if err != nil {
			awsErr, ok := err.(awserr.Error)
			// ValidationError: Stack with id % does not exist
			if ok && awsErr.Code() == "ValidationError" {
				log.Printf("[WARN] Removing CloudFormation stack %s as it's already gone", d.Id())
				d.SetId("")
				return nil
			}

			return err
		}
	*/

	if *resp.StackSet.StackSetId == d.Id() && *resp.StackSet.Status == "DELETED" {
		log.Printf("[DEBUG] Removing CloudFormation stack set %s"+
			" as it has been already deleted", d.Id())
		d.SetId("")
		return nil
	}

	template, err := normalizeCloudFormationTemplate(*resp.StackSet.TemplateBody)
	if err != nil {
		return errwrap.Wrapf("template body contains an invalid JSON or YAML: {{err}}", err)
	}
	d.Set("template_body", template)

	stack := resp.StackSet
	log.Printf("[DEBUG] Received CloudFormation stack set: %s", stack)

	d.Set("name", stack.StackSetName)
	d.Set("arn", stack.StackSetId)

	if stack.Description != nil {
		d.Set("description", stack.Description)
	}

	originalParams := d.Get("parameters").(map[string]interface{})
	err = d.Set("parameters", flattenCloudFormationParameters(stack.Parameters, originalParams))
	if err != nil {
		return err
	}

	err = d.Set("tags", flattenCloudFormationTags(stack.Tags))
	if err != nil {
		return err
	}

	if len(stack.Capabilities) > 0 {
		err = d.Set("capabilities", schema.NewSet(schema.HashString, flattenStringList(stack.Capabilities)))
		if err != nil {
			return err
		}
	}

	return nil
}

func resourceAwsCloudFormationStackSetUpdate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).cfconn
	input := &cloudformation.UpdateStackSetInput{
		StackSetName: aws.String(d.Get("name").(string)),
	}

	// Either TemplateBody, TemplateURL or UsePreviousTemplate are required
	if v, ok := d.GetOk("template_url"); ok {
		input.TemplateURL = aws.String(v.(string))
	}
	if v, ok := d.GetOk("template_body"); ok && input.TemplateURL == nil {
		template, err := normalizeCloudFormationTemplate(v)
		if err != nil {
			return errwrap.Wrapf("template body contains an invalid JSON or YAML: {{err}}", err)
		}
		input.TemplateBody = aws.String(template)
	}

	// Capabilities must be present whether they are changed or not
	if v, ok := d.GetOk("capabilities"); ok {
		input.Capabilities = expandStringList(v.(*schema.Set).List())
	}

	// Parameters must be present whether they are changed or not
	if v, ok := d.GetOk("parameters"); ok {
		input.Parameters = expandCloudFormationParameters(v.(map[string]interface{}))
	}

	if v, ok := d.GetOk("tags"); ok {
		input.Tags = expandCloudFormationTags(v.(map[string]interface{}))
	}
	if v, ok := d.GetOk("description"); ok {
		input.Description = aws.String(v.(string))
	}

	log.Printf("[DEBUG] Updating CloudFormation stack set: %s", input)
	_, err := conn.UpdateStackSet(input)
	if err != nil {
		awsErr, ok := err.(awserr.Error)
		// ValidationError: No updates are to be performed.
		if !ok ||
			awsErr.Code() != "ValidationError" ||
			awsErr.Message() != "No updates are to be performed." {
			return err
		}

		log.Printf("[DEBUG] Current CloudFormation stack has no updates")
	}
	/*
		lastUpdatedTime, err := getLastCfEventTimestamp(d.Id(), conn)
		if err != nil {
			return err
		}
	*/

	wait := resource.StateChangeConf{
		Pending: []string{
			"RUNNING",
			"STOPPING",
		},
		Target: []string{
			"STOPPED",
			"FAILED",
			"SUCCEEDED",
		},
		Timeout:    d.Timeout(schema.TimeoutUpdate),
		MinTimeout: 5 * time.Second,
		Refresh: func() (interface{}, string, error) {

			done := false
			var summaries []*cloudformation.StackSetOperationSummary
			params := &cloudformation.ListStackSetOperationsInput{
				StackSetName: aws.String(d.Id()),
			}
			var resp *cloudformation.ListStackSetOperationsOutput

			for !done {

				resp, err = conn.ListStackSetOperations(params)
				if err != nil {
					log.Printf("[ERROR] Failed to describe stacks: %s", err)
					return nil, "", err
				}
				log.Printf("[DEBUG] Current CloudFormation stack set operations (%d): %q", len(resp.Summaries), resp.Summaries)

				//fmt.Println(resp)
				for _, s := range resp.Summaries {
					summaries = append(summaries, s)
				}
				if resp.NextToken == nil {
					done = true
				} else {
					params.NextToken = resp.NextToken
				}
			}

			var status string

			if len(summaries) == 0 {
				// stack set virgin
				return resp, "SUCCEEDED", err
			}

			log.Printf("[DEBUG] Working on %d CloudFormation stack set operations.", len(summaries))
			for _, s := range summaries {
				if *s.Status == "STOPPING" || *s.Status == "RUNNING" {
					log.Printf("[DEBUG] Found active CloudFormation stack set operation: %q", s)
					return resp, *s.Status, err
				}
				status = *s.Status
			}

			log.Printf("[DEBUG] Current CloudFormation stack status: %q", status)

			return resp, status, err
		},
	}

	_, err = wait.WaitForState()
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] CloudFormation stack %q has been updated", d.Id())

	return resourceAwsCloudFormationStackSetRead(d, meta)
}

func resourceAwsCloudFormationStackSetDelete(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).cfconn

	input := &cloudformation.DeleteStackSetInput{
		StackSetName: aws.String(d.Id()),
	}
	log.Printf("[DEBUG] Deleting CloudFormation stack set %s", input)
	_, err := conn.DeleteStackSet(input)
	if err != nil {
		awsErr, ok := err.(awserr.Error)
		if !ok {
			return err
		}

		if awsErr.Code() == "ValidationError" {
			// Ignore stack which has been already deleted
			return nil
		}
		return err
	}

	var lastStatus string
	wait := resource.StateChangeConf{
		Pending: []string{
			"ACTIVE",
		},
		Target: []string{
			"DELETED",
		},
		Timeout:    d.Timeout(schema.TimeoutDelete),
		MinTimeout: 5 * time.Second,
		Refresh: func() (interface{}, string, error) {
			resp, err := conn.DescribeStackSet(&cloudformation.DescribeStackSetInput{
				StackSetName: aws.String(d.Id()),
			})
			if err != nil {
				awsErr, ok := err.(awserr.Error)
				if !ok {
					return nil, "", err
				}

				log.Printf("[DEBUG] Error when deleting CloudFormation stack set: %s: %s",
					awsErr.Code(), awsErr.Message())

				// ValidationError: Stack with id % does not exist
				if awsErr.Code() == "ValidationError" {
					return resp, "DELETE_COMPLETE", nil
				}
				return nil, "", err
			}

			status := *resp.StackSet.Status
			lastStatus = status
			log.Printf("[DEBUG] Current CloudFormation stack set status: %q", status)

			return resp, status, err
		},
	}

	_, err = wait.WaitForState()
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] CloudFormation stack set %q has been deleted", d.Id())

	d.SetId("")

	return nil
}

// getLastCfEventTimestamp takes the first event in a list
// of events ordered from the newest to the oldest
// and extracts timestamp from it
// LastUpdatedTime only provides last >successful< updated time

/*
func getLastCfEventTimestamp(stackName string, conn *cloudformation.CloudFormation) (
	*time.Time, error) {
	output, err := conn.DescribeStackEvents(&cloudformation.DescribeStackEventsInput{
		StackName: aws.String(stackName),
	})
	if err != nil {
		return nil, err
	}

	return output.StackEvents[0].Timestamp, nil
}

func getCloudFormationRollbackReasons(stackId string, afterTime *time.Time, conn *cloudformation.CloudFormation) ([]string, error) {
	var failures []string

	err := conn.DescribeStackEventsPages(&cloudformation.DescribeStackEventsInput{
		StackName: aws.String(stackId),
	}, func(page *cloudformation.DescribeStackEventsOutput, lastPage bool) bool {
		for _, e := range page.StackEvents {
			if afterTime != nil && !e.Timestamp.After(*afterTime) {
				continue
			}

			if cfStackEventIsFailure(e) || cfStackEventIsRollback(e) {
				failures = append(failures, *e.ResourceStatusReason)
			}
		}
		return !lastPage
	})

	return failures, err
}

func getCloudFormationDeletionReasons(stackId string, conn *cloudformation.CloudFormation) ([]string, error) {
	var failures []string

	err := conn.DescribeStackEventsPages(&cloudformation.DescribeStackEventsInput{
		StackName: aws.String(stackId),
	}, func(page *cloudformation.DescribeStackEventsOutput, lastPage bool) bool {
		for _, e := range page.StackEvents {
			if cfStackEventIsFailure(e) || cfStackEventIsStackDeletion(e) {
				failures = append(failures, *e.ResourceStatusReason)
			}
		}
		return !lastPage
	})

	return failures, err
}

func getCloudFormationFailures(stackId string, conn *cloudformation.CloudFormation) ([]string, error) {
	var failures []string

	err := conn.DescribeStackEventsPages(&cloudformation.DescribeStackEventsInput{
		StackName: aws.String(stackId),
	}, func(page *cloudformation.DescribeStackEventsOutput, lastPage bool) bool {
		for _, e := range page.StackEvents {
			if cfStackEventIsFailure(e) {
				failures = append(failures, *e.ResourceStatusReason)
			}
		}
		return !lastPage
	})

	return failures, err
}

func cfStackEventIsFailure(event *cloudformation.StackEvent) bool {
	failRe := regexp.MustCompile("_FAILED$")
	return failRe.MatchString(*event.ResourceStatus) && event.ResourceStatusReason != nil
}

func cfStackEventIsRollback(event *cloudformation.StackEvent) bool {
	rollbackRe := regexp.MustCompile("^ROLLBACK_")
	return rollbackRe.MatchString(*event.ResourceStatus) && event.ResourceStatusReason != nil
}

func cfStackEventIsStackDeletion(event *cloudformation.StackEvent) bool {
	return *event.ResourceStatus == "DELETE_IN_PROGRESS" &&
		*event.ResourceType == "AWS::CloudFormation::Stack" &&
		event.ResourceStatusReason != nil
}
*/
