import sys
import os

os.system(f"{sys.executable} -m pip install -r requirements.txt")
os.system(f"{sys.executable} -m pip install memory_profiler")
os.system(f"{sys.executable} -m pip install numpy==1.22")
os.system(f"{sys.executable} -m pip install td-ml-cltv")
os.system(f"{sys.executable} -m pip install catboost==1.0.6")
os.system(f"{sys.executable} -m pip install lightgbm")

from td_ml_cltv import *
from catboost import CatBoostRegressor
import lightgbm

# Config parameters
model = os.environ['model']
target = os.environ['target']
scaler = 'MinMax' 
exclude_features = os.environ['exclude_features']        # tuple/list of string column names to exclude from training features

# Declare API Key and db/target tables. These are read from the config.yml file, so make sure database and table names are defined inside the config.yml file and also declared when the Docker Image is loaded in the .dig file
td_api_key = os.environ['TD_API_KEY']
td_api_server= os.environ['TD_API_SERVER']
session_id = os.environ['session_id']
database = os.environ['database']
input_train = os.environ['input_train']
input_test = os.environ['input_test']
canonical_id = os.environ['canonical_id']
feature_importances_table = os.environ['features_table']
run_predictions = os.environ['run_predictions']

#------------------------------------------------
# EXECUTE MODEL
#------------------------------------------------
def build_cltv(model=model, target=target, canonical_id=canonical_id, scaler=scaler, 
              exclude_features = exclude_features, run_predictions=run_predictions):
    """
    model (str: linreg, lgb, cb, xgb): model type to run
    target (str): name of target column
    canonical_id (str): name of unique user identifier column
    scaler (str): what scaler was used in pre-processing
    run_predictions (bool): whether predictions are run on unseen data
    """

    # import data
    df_train = get_table(input_train, database, td_api_key, td_api_server)
    df_train.set_index([canonical_id], inplace = True)

    # cull features
    if type(exclude_features) == str:
        exclude_features = exclude_features.split('|')
    else:
        exclude_features = list(exclude_features)
    to_drop = [item for item in df_train.columns if 'cltv' in item and item !=target]
    to_drop.extend(exclude_features)
    for d in to_drop:
        df_train = df_train.drop(exclude_features, axis=1, errors='ignore')
        
    X_train, y_train = df_train.drop(target, axis=1), df_train[target]
    cols = X_train.columns
    
    # declare model
    if model == 'linreg':
        model = LinearRegression()
    elif model == 'lgb':
        model = lightgbm.LGBMRegressor()
    elif model == 'cb':
        model = CatBoostRegressor(verbose=False)
    else:
        model = XGBRegressor()
        
    # model fitting
    model.fit(X_train, y_train)
    train_pred = model.predict(X_train)

    # explainers
    try:
        impt = impt_coef(model, X_train)
    except:
        impt = impt_tree(model, X_train, canonical_id)
    
    ### UPLOAD OUTPUT TABLES TO TD ###
    client = pytd.Client(apikey=td_api_key, endpoint=td_api_server, database=database)
    train_size, features_count = X_train.shape

    # 1. train & test predictions 
    # (further stats calculations done in Presto)
    preds_train = pd.DataFrame(train_pred, columns = ["predicted_target"], index = X_train.index)
    preds_train['target'] = target
    client.load_table_from_dataframe(preds_train, 'cltv_predictions_train', writer='bulk_import', if_exists='overwrite')
    del df_train 

    # 2. load up test data 
    df_test = get_table(f'{input_test}', database, td_api_key, td_api_server)
    df_test.set_index([canonical_id], inplace = True)
    X_test, y_test = df_test[cols], df_test[target]
    test_size = X_test.shape[0]

    test_pred = model.predict(X_test)
    preds_test = pd.DataFrame(test_pred, columns = ["predicted_target"], index = X_test.index)
    preds_test['target'] = target
    client.load_table_from_dataframe(preds_test, 'cltv_predictions_test', writer='bulk_import', if_exists='overwrite')
    del df_test
    del X_test
    del y_test

    # 3. model summary table
    try:
        params = str(model.get_xgb_params())
    except:
        try:
            params = str(model.get_all_params())
        except:
            params = str(model.get_params())

    model_info = {'session_id': session_id, 
                  'model_type': f'python_{model}', 
                  'runtime': time.asctime(),
                  'model_params': params,
                  'target': target,
                  'features_count': features_count,
                  'train_size': train_size,
                  'test_size': test_size, 
                  'scale_features': scaler}

    params_table = pd.DataFrame([model_info])
    client.load_table_from_dataframe(params_table, 'cltv_model_params', writer='bulk_import', if_exists='append')

    # 4. feature importances
    impt['session_id'] = session_id
    impt['runtime'] = time.asctime()
    client.load_table_from_dataframe(impt, feature_importances_table, writer='bulk_import', if_exists='append')

    # 5. make unseen predictions 
    if run_predictions=='yes':
        df_new = get_table(f'cltv_to_predict', database, td_api_key, td_api_server)
        df_new.set_index([canonical_id], inplace = True)
        X_new, y_new = df_new[cols], df_new[target]

        new_pred = model.predict(X_new)
        preds_new = pd.DataFrame(new_pred, columns = ["predicted_target"], index = X_new.index)
        preds_new['target'] = target
        client.load_table_from_dataframe(preds_new, 'cltv_predictions_unscored', writer='bulk_import', if_exists='overwrite')
        del df_new
        del X_new
        del y_new

