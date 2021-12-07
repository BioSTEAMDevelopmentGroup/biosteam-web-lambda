import json
import os
import sys
from chaospy import distributions as shape

try:
    import boto3
except ImportError:
    from warnings import warn
    warn('boto3 may not be installed; running mock library')

    class MockDynamodb:
        def Table(self, *args, **kwargs): 
            return table
        
    class MockTable:
        creation_date_time = None
        def __init__(self):
            self.items = []
        def put_item(self, *args, **kwargs):
            self.items.append([args, kwargs])

    dynamodb = MockDynamodb()
    table = MockTable()
else:
    # Setting library paths.
    efs_path = "/mnt/simulate"
    python_pkg_path = os.path.join(efs_path, "/lib")
    sys.path.append(python_pkg_path)
    
    # Create dynamodb resource for posting biosteam outputs
    dynamodb = boto3.resource('dynamodb')
    # Imports from EFS library mounted to root /mnt/simulate folder /lib/

# =============================================================================
# Lambda Functions
# =============================================================================

def lambda_handler(event, context):
    model = event['model']
    if model == 'cornstover':
        from biorefineries.cornstover.webapp_model import model
    elif model == 'oilcane':
        import biorefineries.oilcane as oc
        oc.load('O1')
        model = oc.model
    all_parameters = {i.name: i for i in model.parameters}
    
    # Rerun model at baseline to reset cache
    baseline_metrics = model.metrics_at_baseline() 
    
    # Parse input and define data variables 
    jobId = event['jobId']
    jobTimestamp = event['jobTimestamp']
    param_dict = event['params']
    samples_int = event['samples']
    
    parameters = []
    for item in param_dict:
        name = item['name']
        if name in all_parameters:
            parameter = all_parameters[name]
        else:
            raise RuntimeError(f'no parameter with name {name}')
            
        parameters.append(parameter)
        values = item['values']
        distribution = item['distribution'].capitalize()
        if distribution == 'Triangular':
            lower = values['value1']
            midpoint = values['value2']
            upper = values['value3']
            parameter.distribution = shape.Triangle(lower=lower, midpoint=midpoint, upper=upper)
        elif distribution == 'Uniform':
            lower = values['value1']
            upper = values['value2']
            parameter.distribution = shape.Uniform(lower=lower, upper=upper)
        else:
            raise RuntimeError(f"distribution {distribution} not available yet")
    
    # Run model 
    try:
        model.parameters = parameters
        samples = model.sample(N=samples_int, rule='L')
        model.load_samples(samples)
        model.evaluate()
    except Exception as e:
        raise e
    else:
        results = model.table.iloc[:, len(parameters):].copy()
        spearman_rhos, ps = model.spearman_r() 
    finally:
        model.parameters = tuple(all_parameters.values())

    # Format outputs 
    results = results.droplevel('Element', axis=1)
    results_dict = results.to_dict(orient='list')
    results_json = json.dumps(results_dict)
    spearman_rhos_json = spearman_rhos.to_json()

    # Add outputs to DynamoDB table: biosteamJobResults
    jobTimestamp = int(jobTimestamp)
    
    # Instatiate table 
    table = dynamodb.Table('biosteamResults')
    print(table.creation_date_time)
    
    # Add biosteam results to table 
    table.put_item(
      Item={
            'jobId': jobId,
            'jobTimestamp': jobTimestamp,
            'results': results_json,
            'spearmanResults': spearman_rhos_json,
        }
    )

    # Return job status
    return {
        'jobId': jobId,
        'Processed': 'yes',
    }

def test_lambda_handler():
    # Just make sure it runs for now
    # TODO: Add more rigorous tests
    context = None
    event = {
        'model': 'cornstover',
        'jobId': None,
        'jobTimestamp': 1,
        'params': [
            {'name': 'Cornstover price',
             'distribution': 'Uniform',
             'values': {
                 'value1': 0.0464,
                 'value2': 0.0567,
                 },
             },
        ],
        'samples': 50,
    }
    lambda_handler(event, context)
    
    event = {
        'model': 'oilcane',
        'jobId': None,
        'jobTimestamp': 1,
        'params': [
            {'name': 'Cane oil content',
             'distribution': 'Uniform',
             'values': {
                 'value1': 5,
                 'value2': 15,
                 },
             },
        ],
        'samples': 50,
    }
    lambda_handler(event, context)