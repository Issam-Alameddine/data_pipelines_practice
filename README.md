# Creating a Trading Bot
## Objective:
- Create a trading bot that detects anomalies from indicators before a price spike. Using polygon.io as data source.

## NOTES

### Admin
- aws iam list-attached-group-policies --group-name myGroupName

### CloudFormation
- Validate your template:  
   <div align="center"> <pre>aws cloudformation validate-template --template-body file://my_template_file.yaml</pre></div>
- Deploy template:  
   <div align="center"><pre>aws cloudformation create-stack --stack-name myStackName --template-body file://my_template_file.yaml</pre></div>
- Monitor progress:  
   <div align="center"><pre>aws cloudformation describe-stacks --stack-name myStackName --query "Stacks[0].StackStatus</pre></div>
- If stack deployment failed, check the error, delete and verify it's gone:
   <div align="center"><pre>aws cloudformation describe-stack-events --stack-name myStackName --query "StackEvents[*].[ResourceStatus,ResourceStatusReason]"
     aws cloudformation delete-stack --stack-name myStackName
     aws cloudformation list-stacks --query "StackSummaries[?StackStatus=='DELETE_COMPLETE']"</pre>
   </div>
 
### Kinesis
 - list streams: aws kinesis list-streams

