import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import date
from dateutil.relativedelta import relativedelta

# These should be updated whenever changes to the model are made
_meta_model_type = 'sklearn.linear_model.LinearRegression'
_meta_model_version = '1.0'

display_final_dataframe = False

def model(dbt, session):

    dates = []
    end = date.fromisoformat(dbt.config.get("end_date"))
    current = date.fromisoformat(dbt.config.get("start_date"))    
    while current <= end:
        dates.append(current)
        current += relativedelta(months=1)

    df = dbt.ref("int_unattributed_multipliers_aggregated")
    df = df.toPandas()
    x = np.array(df['months_since_2020']).reshape(-1, 1)
    y = df['unattributed_multiplier']
    weights = df['unattributed_conversion_count'] / 40

    regr = LinearRegression()
    regr.fit(x, y, weights)

    final_df = pd.DataFrame()
    final_df['activity_month_at'] = dates
    multipliers = regr.predict(np.array(range(1, 61, 1)).reshape(-1, 1))
    final_df['predicted_unattributed_multiplier'] = multipliers
    
    # The following should not be modified
    final_df['_meta_model_type'] = _meta_model_type
    final_df['_meta_model_version'] = _meta_model_version
    
    if display_final_dataframe:
        display(final_df)

    return final_df
