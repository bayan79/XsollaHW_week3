import math as mt
import datetime as dt

import numpy as np
from scipy.stats import t
import pandas as pd
import pandas_gbq


def getFreshData(Credentials, ProjectId, Fields=None):
    """Funtion for getting fresh data from BigQuery for workload scoring model

    Args:
        Credentials - google service account object with credentials data for project
        ProjectId - string, name of data source project
        Fields = list of strings, name of selected fields

    Example:
        >>> getFreshData(credentials, project_id, ['assignee_id', 'status'])
        id	    created_at	        updated_at	        type	  subject   description	                                                                                          status	requester_id	submitter_id   assignee_id	 id_project	 id_invoice	channel	country	 manual_category  auto_category	 subcategory   feedback_score	feedback_comment
        2520211	2018-02-05 08:59:15	2018-02-23 13:05:39	question	        Credit card payments not working from website.	I have been trying since Thursday 1st of Jan t...	  closed	360790164527	360790164527   20890258907	 21190	     316520736	other	za	     None	          None	         None	       unoffered	    None
        2740781	2018-08-17 01:48:04	2018-09-15 11:00:15	question	        Re: error showed during paid subscription on t...	__________________________________\nType your ... closed	365082895633	951579756	   360133124587	 15174	     367443669	email	za	     None	          None	         None	       offered	        None
    """
    if not Fields:
        Fields = ['assignee_id', 'status']

    bigquery_sql = " ".join([f"SELECT id, DATE(CAST(created_at AS DATETIME)) AS created, DATE(CAST(updated_at AS DATETIME)) AS updated, {',  '.join(Fields)}",
                             "FROM `xsolla_summer_school.customer_support`",
                             "WHERE status IN ('closed','solved')",
                             "ORDER BY updated_at"])

    dataframe = pandas_gbq.read_gbq(
        bigquery_sql, project_id=ProjectId, credentials=Credentials, dialect="standard")

    return dataframe


def workloadScoringByStatuses(Data, NumOfAllDays, NumOfIntervalDays, UseRobast=False, ConfidenceInterval=None):
    """Function for scoring workload by statuses (In Progress and Done) for one employee, NumOfAllDays = 63, NumOfIntervalDays = 7
    Args:
        Data - pandas dataframe object, with hist data for customer support agent
        NumOfAllDays - integer, number of days for all hist data
        NumOfIntervalDays - integer, number of days for weekly calculating interval
        UseRobast - boolean, when true median used insead of mean
        ConfidenceInterval - float, 0 < value < 1, when specified value used as statistical significance

    Example:
        Data = id	   created	   updated	   status	assignee_id
               2936149  2018-12-31  2019-01-30  closed	26979706288
               2936171  2018-12-31  2019-01-30  closed	26979706288

        >>> workloadScoreingByStatuses(Data, 63, 7)
        (<DataFrame scores by status>, <DataFrame total mean scores>)

        <DataFrame scores by status>
            assignee_id	 status	 count_last_period	count_mean_calc_period	count_sem_calc_period	score_value
            12604869947	 closed	 196	            196.62	                9.43	                1
            12604869947	 solved	 0	                0.00	                0.00	                0
        <DataFrame total mean scores>
            assignee_id  score_value
            12604869947  1.0
            12604869947  0.0
    """

    assignees = np.unique(Data.assignee_id)
    statuses = np.unique(Data.status)
    assignee_id_list = []
    status_list = []
    avg_num_of_task_per_week_list = []
    ste_list = []
    num_tasks_per_current_week_list = []
    score_for_status_list = []

    for assignee_id in assignees:
        for status in statuses:
            dataframe_status = Data[(Data['status'] == str(status)) &
                                    (Data['assignee_id'] == assignee_id)][:]

            # time borders params
            curr_date = dt.datetime.strptime(str('2018-04-01'), '%Y-%m-%d')
            curr_date = curr_date.date()
            delta = dt.timedelta(days=NumOfAllDays)
            first_date = curr_date - delta

            # time interval params
            delta_interval = dt.timedelta(days=NumOfIntervalDays)
            first_interval = first_date + delta_interval

            num_of_intervals = int(NumOfAllDays / NumOfIntervalDays)
            num_tasks_per_week = []

            for i in range(0, num_of_intervals):
                interval = dataframe_status[(dataframe_status['updated'] >= str(first_date)) &
                                            (dataframe_status['updated'] <= str(first_interval))][:]
                first_date = first_date + delta_interval
                first_interval = first_interval + delta_interval

                if i != (num_of_intervals - 1):
                    num_of_tasks = len(np.unique(interval['id']))
                    # history number of tasks
                    num_tasks_per_week.append(num_of_tasks)
                else:
                    num_tasks_per_current_week = len(
                        np.unique(interval['id']))  # currently number of tasks

            avg_num_of_task_per_week = round(np.mean(num_tasks_per_week), 2)

            # squared deviations
            x_values = []
            for num in num_tasks_per_week:
                x = round((num - avg_num_of_task_per_week)**2, 2)
                x_values.append(x)

            # data sampling statistics
            x_sum = round(sum(x_values), 2)  # sum of squared deviations
            dispersion = round(x_sum / (num_of_intervals - 1), 2)  # dispersion
            # standart deviation for sample
            std = round(mt.sqrt(dispersion), 2)
            # standart error for sample
            ste = round(std / mt.sqrt(num_of_intervals), 2)

            if UseRobast:
                central_value = np.median(num_tasks_per_week)    
            else:
                central_value = np.mean(num_tasks_per_week)

            if ConfidenceInterval:                
                size = len(x_values)
                t_crit = t.ppf(1 - ConfidenceInterval/2, size - 1)
                delta = t_crit*std/np.sqrt(size)
            else:
                delta = ste
            # confidence interval
            left_border = int(central_value - delta)
            right_border = int(central_value + delta)
            
            # workload scoring for status
            if ConfidenceInterval:
                score_for_status = workloadScoreStatuses(left_border, right_border, num_tasks_per_current_week)
            else:
                score_for_status = workloadScoreStatuses(left_border, right_border, num_tasks_per_current_week)

            assignee_id_list.append(assignee_id)
            status_list.append(status)
            avg_num_of_task_per_week_list.append(avg_num_of_task_per_week)
            ste_list.append(ste)
            num_tasks_per_current_week_list.append(num_tasks_per_current_week)
            score_for_status_list.append(score_for_status)

    score_data = {"assignee_id": assignee_id_list,
                  "status": status_list,
                  "count_last_period": num_tasks_per_current_week_list,
                  "count_mean_calc_period": avg_num_of_task_per_week_list,
                  "count_sem_calc_period": ste_list,
                  "score_value": score_for_status_list}

    scores = pd.DataFrame(data=score_data)
    scores_total = scores.groupby('assignee_id')['score_value'].mean().reset_index()

    return scores, scores_total


def workloadScoringByStatusesChannels(Data, NumOfAllDays, NumOfIntervalDays, UseRobast=False, ConfidenceInterval=None):
    """Function for scoring workload by statuses (In Progress and Done) for one employee, NumOfAllDays = 63, NumOfIntervalDays = 7
    Args:
        Data - pandas dataframe object, with hist data for customer support agent
        NumOfAllDays - integer, number of days for all hist data
        NumOfIntervalDays - integer, number of days for weekly calculating interval
        UseRobast - boolean, when true median used insead of mean
        ConfidenceInterval - float, 0 < value < 1, when specified value used as statistical significance

    Example:
        Data = id       created     updated     status  channel assignee_id
               2936149  2018-12-31  2019-01-30  closed  chat    26979706288
               2936171  2018-12-31  2019-01-30  closed  email   26979706288

        >>> workloadScoringByStatusesChannels(Data, 63, 7, UseRobust=True, ConfidenceInterval=0.05)
        (<DataFrame scores by status and channels>, <DataFrame total mean scores>)

        <DataFrame scores by status and channels>
            assignee_id  status  count_last_period  count_mean_calc_period  count_sem_calc_period   score_value
            12604869947  closed  196                196.62                  9.43                    1
            12604869947  solved  0                  0.00                    0.00                    0
        <DataFrame total mean scores>
            assignee_id  score_value
            12604869947  1.0
            12604869947  0.0
    """
    curr_date = dt.datetime.strptime(str('2018-04-01'), '%Y-%m-%d')
    num_of_intervals = NumOfAllDays // NumOfIntervalDays

    # select period from whole dataset
    Data['days_delta'] = curr_date - pd.to_datetime(Data['updated'], format="%Y-%m-%d")
    Data['week_num'] = Data['days_delta'].dt.days // NumOfIntervalDays

    df_period = Data[((Data['days_delta'].dt.days >= 0) &
                      (Data['days_delta'].dt.days < NumOfAllDays))]

    # group by (features, week_num) and calculate count in groups
    counted = df_period.groupby(['assignee_id', 'status', 'channel', 'week_num']).nunique('id')[['id']].reset_index()

    # split on current and previos
    previous = counted[counted['week_num'] != 0].rename({'id': 'count'}, axis=1)
    current = counted[counted['week_num'] == 0].drop(
        'week_num', axis=1).rename({'id': 'count_last_period'}, axis=1)

    def get_group_index(row):
        """Extract from row a group index"""
        return (row['assignee_id'], row['status'], row['channel'])

    # count central value foreach group
    groups = previous.groupby(['assignee_id', 'status', 'channel'])['count']
    if UseRobast:
        central_values = groups.median()
    else:
        central_values = groups.mean()

    # count std, ste foreach group
    previous['squared_deviation'] = previous.apply(
        lambda x: (x['count'] - central_values[get_group_index(x)])**2, axis=1)
    sums_sqr_deviations = previous.groupby(['assignee_id', 'status', 'channel'])[
        'squared_deviation'].sum()
    dispertion = sums_sqr_deviations / (num_of_intervals - 1)
    std = np.sqrt(dispertion)
    ste = std / np.sqrt(num_of_intervals)

    # add mean and ste to dataframe
    current['count_mean_calc_period'] = current.apply(
        lambda x: central_values.get(get_group_index(x), 0), axis=1)
    current['count_sem_calc_period'] = current.apply(
        lambda x: ste.get(get_group_index(x), 0), axis=1)

    def get_delta(row):
        """Return interval distance from central value"""
        if ConfidenceInterval:
            row_group = get_group_index(row)
            
            size_group = groups.count().get(row_group, 0)
            t_crit = t.ppf(1-ConfidenceInterval/2, size_group-1)
            std_group = std.get(row_group, 0)

            return t_crit*std_group/np.sqrt(size_group)
        else:
            return ste.get(get_group_index(row), 0)

    # calc borders and final score
    current['left_border'] = current.apply(
        lambda x: central_values.get(get_group_index(x), 0) - get_delta(x), axis=1)
    current['right_border'] = current.apply(
        lambda x: central_values.get(get_group_index(x), 0) + get_delta(x), axis=1)
    current['score_value'] = current.apply(
        lambda x: workloadScoreStatuses(x['left_border'], x['right_border'], x['count_last_period']), axis=1)
    
    current.drop(['left_border', 'right_border'], inplace=True, axis=1)

    # calc total score by assignee
    scores_total = current.groupby('assignee_id')['score_value'].mean().reset_index()

    return current, scores_total


def workloadScoreStatuses(LeftBoard, RightBoard, CurrentNumOfTasks):
    """Function for scoring workload for current status

    Args:
        LeftBoard - float, left boarder for confidence interval
        RightBoard - float right boarder for confidence interval
        CurrentNumOfTasks - integer, number of customer support agent tasks for current interval (7 days)

    Example:
        >>> workloadScoreStatuses(187, 206, 196)
        1
    """
    if (LeftBoard == 0) & (CurrentNumOfTasks == 0) & (RightBoard == 0):
        score = 0
    elif (CurrentNumOfTasks >= 0) & (CurrentNumOfTasks < LeftBoard):
        score = 0
    elif (CurrentNumOfTasks >= LeftBoard) & (CurrentNumOfTasks <= RightBoard):
        score = 1
    else:
        score = 2

    return score


def insertScoreResultData(InsertDataFrame, ProjectId, DatasetId, TableId):
    """Function for inserting data to BigQuery database

    Args:
        InsertDataFrame - pandas dtaframe object, with score result data by statuses
        ProjectId - string, name of project in google cloud platform 
        DatasetId - string, name of dataset in bigquery for raw data
        TableId - string, name of table for raw data

    Example:
        InsertDataFrame = assignee_id    status  count_last_period   count_mean_calc_period  count_sem_calc_period   score_value
                          11527290367    closed  163                 140.38                  12.4                    2
                          11527290367    solved  0                   0.00                    0.0                     0
        >>> insertScoreResultData(InsertDataFrame, 'test-gcp-project', 'test_dataset', 'test_table')
    """
    destination_table = f"{DatasetId}.{TableId}"

    types_mapper = {
        'int64': 'int',
        'float64': 'float',
    }

    res_df = pd.DataFrame()
    for column in InsertDataFrame.columns:
        column_type = str(InsertDataFrame[column].dtype)
        db_column_type = types_mapper.get(column_type, default='str')
        res_df[column] = InsertDataFrame[column].astype(db_column_type)

    res_df['developer'] = 'kirill.bayandin'
    res_df['developer'] = res_df['developer'].astype('str')

    pandas_gbq.to_gbq(res_df, destination_table=destination_table,
                      project_id=ProjectId, if_exists='append')
