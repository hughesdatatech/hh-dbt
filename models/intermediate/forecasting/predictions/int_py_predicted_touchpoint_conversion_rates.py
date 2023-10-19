import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split as tts
import statsmodels.api as sm

# The model type and version should be updated whenever changes are made to the model
# The features will be set based on whatever features are input to the model
_meta_model_type = 'statsmodels.regression.linear_model.WLS'
_meta_model_version = '5.0'

display_final_dataframe = False
display_model_summaries = False

def prep_variables(df, ftr, dummy_features, add_target=True):
    data = pd.DataFrame(data=df, columns=ftr)
    dummies = pd.get_dummies(df[dummy_features], drop_first=True, dtype=int)
    data = pd.concat([data, dummies], axis=1)
    if add_target:
        data['target'] = df['conversion_rate']
    return data

def model(dbt, session):

    #########################
    # First Deployments Model
    #########################

    first_features = [
        'is_email_communication',
        'is_customer_communication',
        'is_touchpoint_sender_hh',
        'is_customer_fully_insured',
        'is_new_customer_incentive',
        'approximate_scheduled_send_count'
    ]
    first_dummy_features = [
        'month_of_send_date',
        'population_type',
        'touchpoint_number',
        'customer_membership_size',
        'customer_tier'
    ]

    first_df = dbt.ref("int_yield_first_deployments")
    first_df = first_df.toPandas()

    first_features.append('touchpoint_id')
    first_data = prep_variables(first_df, first_features, first_dummy_features)
    
    first_removed_features = [
        'customer_membership_size_50,000+',
        'customer_membership_size_2,000-9,999',
        'customer_membership_size_Less than 2,000',
        'customer_membership_size_Unknown',
        'is_touchpoint_sender_hh'
    ]

    first_data = first_data.drop(columns=first_removed_features)
    first_x = first_data.drop(['target'], axis=1)
    first_y = first_data.target
    first_x = sm.add_constant(first_x)

    first_x_train, first_x_test, first_y_train, first_y_test = tts(first_x, first_y, random_state = 0, test_size=0.001)
    first_x_train = first_x_train.drop(['touchpoint_id'], axis=1)
    first_x_train = sm.add_constant(first_x_train)
    first_mod = sm.WLS(first_y_train, first_x_train.drop(['approximate_scheduled_send_count'], axis=1), weights=first_x_train.approximate_scheduled_send_count)
    first_mod_res = first_mod.fit()

    first_x_train.drop(['approximate_scheduled_send_count'], axis=1, inplace=True)
    _meta_first_trained_features = 'predicted using first deployment features: ' + '|'.join(list(first_x_train.columns))
    
    if display_model_summaries:
        display(first_mod_res.summary())

    ##########################
    # Repeat Deployments Model
    ##########################

    repeat_features = [
        'is_email_communication',
        'is_customer_communication',
        'is_touchpoint_sender_hh',
        'is_customer_fully_insured',
        'approximate_scheduled_send_count',
        'pct_employee',
        'pct_female',
        'first_conversion_rate',
        'last_conversion_rate',
        'age_of_touchpoint'
    ]
    repeat_dummy_features = [
        'month_of_send_date',
        'population_type',
        'touchpoint_number',
        'customer_membership_size',
        'customer_tier'
    ]

    repeat_df = dbt.ref("int_yield_repeat_deployments")
    repeat_df = repeat_df.toPandas()

    repeat_features.append('touchpoint_id')
    repeat_data = prep_variables(repeat_df, repeat_features, repeat_dummy_features)
    repeat_removed_features = [
        'customer_membership_size_50,000+',
        'customer_membership_size_2,000-9,999',
        'customer_membership_size_Less than 2,000',
        'customer_membership_size_Unknown',
        'is_touchpoint_sender_hh',
        'pct_employee',
        'pct_female'
    ]
    
    repeat_data = repeat_data.drop(columns=repeat_removed_features)
    repeat_x = repeat_data.drop(['target'], axis=1)
    repeat_y = repeat_data.target

    repeat_x_train, repeat_x_test, repeat_y_train, repeat_y_test = tts(repeat_x, repeat_y, random_state = 0, test_size=0.001)
    repeat_x_train = repeat_x_train.drop(['touchpoint_id'], axis=1)
    repeat_x_train = sm.add_constant(repeat_x_train)
    repeat_mod = sm.WLS(repeat_y_train, repeat_x_train.drop(['approximate_scheduled_send_count'], axis=1), weights=repeat_x_train.approximate_scheduled_send_count)
    repeat_mod_res = repeat_mod.fit()

    repeat_x_train.drop(['approximate_scheduled_send_count'], axis=1, inplace=True)
    _meta_repeat_trained_features = 'predicted using repeat deployment features: ' + '|'.join(list(repeat_x_train.columns))

    if display_model_summaries:
        display(repeat_mod_res.summary())

    ##########################
    # Run Model Using All Data
    ##########################

    final_df = dbt.ref("int_yield_all_touchpoints")
    final_df = final_df.toPandas()

    # Formatting input data for predictions

    first_deployment = final_df.loc[(final_df['repeat_deployment']==0) | (np.isnan(final_df['first_conversion_rate'])) | (np.isnan(final_df['last_conversion_rate']))]
    repeat_deployment = final_df[(final_df['repeat_deployment']==1) & (~np.isnan(final_df['first_conversion_rate'])) & (~np.isnan(final_df['last_conversion_rate']))]
    first_input = prep_variables(first_deployment, first_features, first_dummy_features, False)
    repeat_input = prep_variables(repeat_deployment, repeat_features, repeat_dummy_features, False)

    first_input = first_input.drop(first_removed_features, axis=1)
    repeat_input = repeat_input.drop(repeat_removed_features, axis=1)
    first_input = first_input.drop(['touchpoint_id', 'approximate_scheduled_send_count'], axis=1)
    repeat_input = repeat_input.drop(['touchpoint_id', 'approximate_scheduled_send_count'], axis=1)
    first_input = sm.add_constant(first_input)
    repeat_input = sm.add_constant(repeat_input)
    
    # Run predictions

    first_conversion_rates = first_mod_res.predict(first_input)
    repeat_conversion_rates = repeat_mod_res.predict(repeat_input)
    all_conversion_rates = pd.concat([first_conversion_rates, repeat_conversion_rates])

    # Formatting output

    final_df['predicted_conversion_rate'] = all_conversion_rates.clip(lower=0)
    final_df['in_holdout'] = final_df['touchpoint_id'].isin(first_x_test['touchpoint_id'].tolist() + repeat_x_test['touchpoint_id'].tolist())

    # The following should not be modified
    final_df['_meta_model_type'] = _meta_model_type
    final_df['_meta_model_version'] = _meta_model_version
    final_df.loc[(final_df['repeat_deployment']==0) | (np.isnan(final_df['first_conversion_rate'])) | (np.isnan(final_df['last_conversion_rate'])), '_meta_model_features'] = _meta_first_trained_features
    final_df.loc[(final_df['repeat_deployment']==1) & (~np.isnan(final_df['first_conversion_rate'])) & (~np.isnan(final_df['last_conversion_rate'])), '_meta_model_features'] = _meta_repeat_trained_features
    
    if display_final_dataframe:
        display(final_df)

    return final_df
