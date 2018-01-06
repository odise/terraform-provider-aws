package aws

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudformation"
	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform/helper/schema"
)

func dataSourceAwsCloudFormationStackSet() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceAwsCloudFormationStackRead,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"template_body": {
				Type:     schema.TypeString,
				Computed: true,
				StateFunc: func(v interface{}) string {
					template, _ := normalizeCloudFormationTemplate(v)
					return template
				},
			},
			"capabilities": {
				Type:     schema.TypeSet,
				Computed: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Set:      schema.HashString,
			},
			"description": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"parameters": {
				Type:     schema.TypeMap,
				Computed: true,
			},
			"timeout_in_minutes": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"tags": {
				Type:     schema.TypeMap,
				Computed: true,
			},
		},
	}
}

func dataSourceAwsCloudFormationStackSetRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).cfconn
	name := d.Get("name").(string)
	input := &cloudformation.DescribeStackSetInput{
		StackSetName: aws.String(name),
	}

	log.Printf("[DEBUG] Reading CloudFormation StackSet: %s", input)

	out, err := conn.DescribeStackSet(input)
	if err != nil {
		return fmt.Errorf("Failed describing CloudFormation stack set (%s): %s", name, err)
	}

	stack := out.StackSet

	d.SetId(*stack.StackSetId)

	d.Set("description", stack.Description)

	d.Set("parameters", flattenAllCloudFormationParameters(stack.Parameters))
	d.Set("tags", flattenCloudFormationTags(stack.Tags))

	if len(stack.Capabilities) > 0 {
		d.Set("capabilities", schema.NewSet(schema.HashString, flattenStringList(stack.Capabilities)))
	}

	tInput := cloudformation.GetTemplateInput{
		StackName: aws.String(name),
	}
	tOut, err := conn.GetTemplate(&tInput)
	if err != nil {
		return err
	}

	template, err := normalizeCloudFormationTemplate(*tOut.TemplateBody)

	if err != nil {
		return errwrap.Wrapf("template body contains an invalid JSON or YAML: {{err}}", err)
	}
	d.Set("template_body", template)

	return nil
}
