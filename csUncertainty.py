import json
import os
import sys
import boto3

# Setting library paths.
efs_path = "/mnt/simulate"
python_pkg_path = os.path.join(efs_path, "/lib")
sys.path.append(python_pkg_path)

#create dynamodb resource for posting biosteam outputs
dynamodb = boto3.resource('dynamodb')

#imports from EFS library mounted to root /mnt/simulate folder /lib/
import biosteam as bst
from chaospy import distributions as shape
from biosteam.evaluation import Model, Metric
import biorefineries.cornstover as cs
import pandas as pd

# =============================================================================
# Define Metric functions
# =============================================================================

cs_sys = cs.cornstover_sys
cs_sys.simulate()
cs_tea = cs.cornstover_tea

ethanol_density_kggal = cs.ethanol_density_kggal
ethanol = cs.ethanol
get_MESP = lambda: cs_tea.solve_price(ethanol) * ethanol_density_kggal

# Convert from kg/hr to MM gal/yr
get_ethanol_production = \
    lambda: ethanol.F_mass/ethanol_density_kggal*cs_tea.operating_days*24/1e6

# Yield in gal/dry-U.S.ton, 907.185 just kg to ton
cornstover = cs.cornstover
get_ethanol_yield = lambda: (ethanol.F_mass/ethanol_density_kggal) / \
    ((cornstover.F_mass-cornstover.imass['H2O'])/907.185)

# Total capital investment in MM$
get_TCI = lambda: cs_tea.TCI/1e6

# Annual operating cost
get_AOC = lambda: cs_tea.AOC/1e6

# If power_utility.rate is negative, it means consumption < production
get_net_electricity = lambda: -sum(i.power_utility.rate for i in cs.AllAreas.units)/1e3

get_electricity_credit = lambda: -cs_tea.utility_cost/1e6

metrics =[Metric('Minimum ethanol selling price', get_MESP, '$/gal'),
          Metric('Ethanol production', get_ethanol_production, 'MM gal/yr'),
          Metric('Ethanol yield', get_ethanol_yield, 'gal/dry-US ton'),
          Metric('Total capital investment', get_TCI, 'MM$'),
          Metric('Annual operating cost', get_AOC, 'MM$/yr'),
          Metric('Net electricity', get_net_electricity, 'MW'),          
          Metric('Electricity credit', get_electricity_credit, 'MM $/yr')]


#define parameters for lambda function 
R201 = cs.R201
R301 = cs.R301
BT = cs.BT

# =============================================================================
# Lambda Function
# =============================================================================

def lambda_handler(event, context):

    #parse input and define data variables 
    # body = event['body']
    # input = json.loads(event)
    jobId = event['jobId']
    jobTimestamp = event['jobTimestamp']
    param_dict = event['params']
    samples_int = event['samples']
    
    #initiate model
    cs_model = Model(cs_sys, metrics)
    param = cs_model.parameter
    
    #add uncertainty parameters 
    for item in param_dict:

        #specify distribution
        if item['distribution'] == 'Triangular':
            lower = item['values']['value1']
            midpoint = item['values']['value2']
            upper = item['values']['value3']
            b = midpoint
            D = shape.Triangle(lower=lower, midpoint=midpoint, upper=upper)
        else:
            lower = item['values']['value1']
            upper = item['values']['value2']
            b = (upper + lower)/2
            D = shape.Uniform(lower=lower, upper=upper)

        #set parameters 
        if item['name'] == 'Cornstover price':             
            @param(name='Cornstover price', element=cornstover, kind='isolated', units='$/kg',
                baseline=b, distribution=D)
            def set_cornstover_price(price):
                cornstover.price = price

        elif item['name'] == 'Enzyme price':
            @param(name='Enzyme price', element=cs.cellulase, kind='isolated', units='$/kg',
                baseline=b, distribution=D)
            def set_cellulase_price(price):
                cs.cellulase.price = price
            
        elif item['name'] == 'Electricity price':          
            @param(name='Electricity price', element='TEA', kind='isolated', units='$/kWh',
                baseline=b, distribution=D)
            def set_electricity_price(price):
                bst.PowerUtility.price = price    

        elif item['name'] == 'Income tax':           
            @param(name='Income tax', element='TEA', kind='isolated', units='%',
                baseline=b, distribution=D)
            def set_tax_rate(rate):
                cs_tea.income_tax = rate      
                
        elif item['name'] == 'Plant size':              
            @param(name='Plant size', element=cornstover, kind='coupled', units='kg/hr',
                baseline=b, distribution=D)
            def set_plant_size(flow_rate):
                cornstover.F_mass = flow_rate
            
        elif item['name'] == 'PT glucose-to-glucose':          
            @param(name='PT glucose-to-glucose', element=R201, kind='coupled', units='%',
                baseline=b, distribution=D)
            def set_PT_glucan_to_glucose(X):
                R201.reactions[0].X = X
                
        elif item['name'] == 'PT xylan-to-xylose':     
            @param(name='PT xylan-to-xylose', element=R201, kind='coupled', units='%',
                baseline=b, distribution=D)
            def set_PT_xylan_to_xylose(X):
                R201.reactions[8].X = X
            
        elif item['name'] == 'Xylan-to-furfural conversion':
            # b = R201.reactions[10].X
            # Yoel had a bug in the script, the original X should be 0.05 but he put in 0.005,
            # I submitted an issue in GitHub to ask him to fix it 
            @param(name='Xylan-to-furfural conversion', element=R201, kind='coupled', units='%',
                baseline=0.05, distribution=D)
            def set_PT_xylan_to_furfural(X):
                # To make sure the overall xylan conversion doesn't exceed 100%
                R201.reactions[10].X = min((1-R201.reactions[8].X-R201.reactions[9].X), X)
                
        elif item['name'] == 'EH cellulose-to-glucose':
            @param(name='EH cellulose-to-glucose', element=R301, kind='coupled', units='%',
                baseline=b, distribution=D)
            def set_EH_glucan_to_glucose(X):
                R301.saccharification[2].X = X
            
        elif item['name'] == 'EH time':
            @param(name='EH time', element=R301, kind='isolated', units='hr',
                baseline=b, distribution=D)
            def set_EH_time(tau):
                R301.tau_saccharification = tau
                
        elif item['name'] == 'FERM glucose-to-ethanol':
            @param(name='FERM glucose-to-ethanol', element=R301, kind='coupled', units='%',
                baseline=b, distribution=D)
            def set_FERM_glucose_to_ethanol(X):
                R301.cofermentation[0].X = X
                
        elif item['name'] == 'FERM time':
            @param(name='FERM time', element=R301, kind='isolated', units='hr',
                baseline=b, distribution=D)
            def set_FERM_time(tau):
                R301.tau_cofermentation = tau

        elif item['name'] == 'Boiler efficiency':
            @param(name='Boiler efficiency', element=BT, kind='coupled', units='%',
                baseline=b, distribution=D)
            def set_boiler_efficiency(X):
                BT.boiler_efficiency = X
                
        elif item['name'] == 'Turbogenerator efficiency':
            @param(name='Turbogenerator efficiency', element=BT, kind='coupled', units='%',
                baseline=b, distribution=D)
            def set_turbogenerator_efficiency(X):
                BT.turbogenerator_efficiency = X
    

    # Run model 
    samples = cs_model.sample(N=samples_int, rule='L')
    cs_model.load_samples(samples)
    cs_model.evaluate()
    params = cs_model.get_parameters()
    results = cs_model.table.iloc[:, len(params):].copy()
    spearman_metrics = metrics
    cs_model.table = cs_model.table.dropna()
    spearman_params = params
    spearman_results = cs_model.spearman(spearman_params, spearman_metrics) 

    # Format outputs 
    results = results.droplevel('Element', axis=1)
    results_dict = results.to_dict(orient='list')
    results_json = json.dumps(results_dict)
    spearman_results = spearman_results.droplevel('Element', axis=1)
    spearman_results = spearman_results.droplevel('Element')
    spearman_results_json = spearman_results.to_json()

    #Add outputs to DynamoDB table: biosteamJobResults
    jobTimestamp = int(jobTimestamp)
    
    #instatiate table 
    table = dynamodb.Table('biosteamResults')
    print(table.creation_date_time)
    
    #add biosteam results to table 
    table.put_item(
      Item={
            'jobId': jobId,
            'jobTimestamp': jobTimestamp,
            'results': results_json,
            'spearmanResults': spearman_results_json,
        }
    )
    
    #Empty the model
    cs_model = None

    #Return job status
    return {
        'jobId' :   jobId,
        'Processed'   :   'yes',
    }
    