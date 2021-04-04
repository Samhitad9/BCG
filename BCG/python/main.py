import sys
import os
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, dense_rank, countDistinct, trim
import helper


# file_config = sys.argv[1]
file_config = 'D:/Samhita/PycharmProjects/BCG/configuration/config.ini'
prop = helper.read_all_properties(file_config)
folder_path = prop['INPUT_FOLDER']

spark_obj = SparkSession.builder \
    .appName('analysis').getOrCreate()


def create_tables(folder_path):
    """
    :param folder_path:
    :type folder_path:
    :return:
    :rtype:
    """
    file_names = os.listdir(folder_path)
    df_charges_ip = spark_obj.read.format('csv').option('header', 'true')\
        .load(folder_path + 'Charges_use.csv')
    df_damages_ip = spark_obj.read.format('csv').option('header', 'true')\
        .load(folder_path + 'Damages_use.csv')
    df_endorse_ip = spark_obj.read.format('csv').option('header', 'true')\
        .load(folder_path + 'Endorse_use.csv')
    df_prim_person_ip = spark_obj.read.format('csv').option('header', 'true')\
        .load(folder_path + 'Primary_Person_use.csv')
    df_restrict_ip = spark_obj.read.format('csv').option('header', 'true')\
        .load(folder_path + 'Restrict_use.csv')
    df_units_ip = spark_obj.read.format('csv').option('header', 'true')\
        .load(folder_path + 'Units_use.csv')
    return df_charges_ip, df_damages_ip, df_units_ip, df_endorse_ip, df_prim_person_ip, df_restrict_ip


def num_crashes_gender_accidents(df, injury_type, gender):
    """
    :param df:
    :type df:
    :param injury_type:
    :type injury_type:
    :param gender:
    :type gender:
    :return:
    :rtype:
    """
    count_crashes = df.filter((df['PRSN_INJRY_SEV_ID'] == injury_type) & (df['PRSN_GNDR_ID'] == gender)) \
        .drop_duplicates().count()
    return count_crashes


def num_two_whlrs_bkd_for_crash(df, vehicle_list):
    """
    :param df:
    :type df:
    :param vehicle_list:
    :type vehicle_list:
    :return:
    :rtype:
    """
    count_two_wheelers = df.where(df['VEH_BODY_STYL_ID'].isin(vehicle_list)) \
        .drop_duplicates().count()
    return count_two_wheelers


def state_with_highest_accidents_gender(df, gender):
    """
    :param df:
    :type df:
    :param gender:
    :type gender:
    :return:
    :rtype:
    """
    state_df = df.filter(df['PRSN_GNDR_ID'] == gender).drop_duplicates(['CRASH_ID']) \
        .groupBy('DRVR_LIC_STATE_ID').count()
    state = state_df.orderBy(desc('count')).take(1)[0][0]
    return state


def veh_make_highest_injury(df, top_n_start, top_n_end):
    """
    :param df:
    :type df:
    :return:
    :rtype:
    """
    wspec = Window.orderBy(desc('count'))
    injury_df = df.filter((df['TOT_INJRY_CNT'] != 0) | (df['DEATH_CNT'] != 0)).drop_duplicates() \
        .groupBy('VEH_MAKE_ID').count()
    injury_df1 = injury_df.withColumn('rank', dense_rank().over(wspec))
    veh_list_obj = injury_df1.filter((injury_df1['rank'] >= top_n_start)
                                     & (injury_df1['rank'] <= top_n_end))\
        .select('VEH_MAKE_ID').collect()
    vehicle_list = [row['VEH_MAKE_ID'] for row in veh_list_obj]
    return list(enumerate(vehicle_list, start=1))


def top_ethnic_group_veh_bdy_style(df_unit, df_person):
    """
    :param df_unit:
    :type df_unit:
    :param df_person:
    :type df_person:
    :return:
    :rtype:
    """
    df_joined = df_unit.join(df_person, ['CRASH_ID'], 'left').drop_duplicates()
    wspec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(desc('count'))
    intd_df = df_joined.groupBy(['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID']).count()
    intd_df1 = intd_df.withColumn('rank', dense_rank().over(wspec))
    final_df_result = intd_df1.filter(intd_df1['rank'] == 1)\
        .select(['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID'])
    return final_df_result


def top_zip_crashes_alcohol(df_unit, df_person, top_n):
    """
    :param df_unit:
    :type df_unit:
    :param df_person:
    :type df_person:
    :param top_n:
    :type top_n:
    :return:
    :rtype:
    """
    df_joined = df_person.join(df_unit, ['CRASH_ID'], 'left') \
        .select(['CRASH_ID', 'DRVR_ZIP', 'CONTRIB_FACTR_1_ID',
                 'CONTRIB_FACTR_2_ID', 'CONTRIB_FACTR_P1_ID']) \
        .drop_duplicates()
    wspec = Window.orderBy(desc('crash_count'))
    intd_df = df_joined.filter((df_joined['CONTRIB_FACTR_1_ID'].like('%DRINKING%')) |
                        (df_joined['CONTRIB_FACTR_1_ID'].like('%ALCOHOL%')) |
                        (df_joined['CONTRIB_FACTR_2_ID'].like('%DRINKING%')) |
                        (df_joined['CONTRIB_FACTR_2_ID'].like('%ALCOHOL%')) |
                        (df_joined['CONTRIB_FACTR_P1_ID'].like('%DRINKING%')) |
                        (df_joined['CONTRIB_FACTR_P1_ID'].like('%ALCOHOL%')))
    intd_df1 = intd_df.groupBy('DRVR_ZIP').agg(countDistinct('CRASH_ID').alias('crash_count')) \
        .orderBy(desc('crash_count')).dropna()
    intd_df2 = intd_df1.withColumn('rank', dense_rank().over(wspec))
    driver_zip_obj = intd_df2.filter(intd_df2['rank'] <= top_n).collect()
    list_driver_zip = [row['DRVR_ZIP'] for row in driver_zip_obj]
    return list(enumerate(list_driver_zip, start=1))


def count_crash_id_no_damage(df_damage, df_unit):
    """
    :param df_damage:
    :type df_damage:
    :param df_unit:
    :type df_unit:
    :return:
    :rtype:
    """
    joined_df = df_damage.filter(df_damage['DAMAGED_PROPERTY'].isNull()) \
        .join(df_unit, ['CRASH_ID'], 'inner')
    intd_df = joined_df.filter(
        (joined_df['VEH_DMAG_SCL_2_ID'].like('%5%')) | (joined_df['VEH_DMAG_SCL_2_ID'].like('%6%'))
        | (joined_df['VEH_DMAG_SCL_2_ID'].like('%7%')))
    count_crash = intd_df.filter(intd_df['FIN_RESP_TYPE_ID'].like('%INSURANCE%')) \
        .agg(countDistinct('CRASH_ID')).rdd.collect()[0][0]
    return count_crash


def analysis_eight(df_unit, df_person, top_n_color, top_n_states, top_n_veh_make):
    """
    :param df_unit:
    :type df_unit:
    :param df_person:
    :type df_person:
    :param top_n_color:
    :type top_n_color:
    :param top_n_states:
    :type top_n_states:
    :param top_n_veh_make:
    :type top_n_veh_make:
    :return:
    :rtype:
    """

    df_units_fil = df_unit.filter((df_unit['CONTRIB_FACTR_1_ID'].like('%SPEED%')) |
                                  (df_unit['CONTRIB_FACTR_2_ID'].like('%SPEED%')) |
                                  (df_unit['CONTRIB_FACTR_P1_ID'].like('%SPEED%')))
    joined_df = df_units_fil.join(df_person, ['CRASH_ID'], 'inner')
    intd_df = joined_df.filter((joined_df['PRSN_TYPE_ID'] == 'DRIVER') &
                               (~joined_df['DRVR_LIC_TYPE_ID']
                                .isin('NA', 'UNLICENSED', 'UNKNOWN', 'OTHER'))
                               & (~joined_df['DRVR_LIC_CLS_ID']
                                  .isin('NA', 'UNLICENSED', 'UNKNOWN', 'OTHER/OUT OF STATE')))
    top_used_veh_color = df_unit.filter(df_unit['VEH_COLOR_ID'] != 'NA').groupBy('VEH_COLOR_ID') \
        .agg(countDistinct('CRASH_ID').alias('color_count')).orderBy(desc('color_count')).take(top_n_color)
    top_used_veh_color_list = [row['VEH_COLOR_ID'] for row in top_used_veh_color]
    intd_df1 = intd_df.where(col('VEH_COLOR_ID').isin(top_used_veh_color_list))
    top_states_with_hig_offnces = df_unit.groupBy('VEH_LIC_STATE_ID')\
        .agg(countDistinct('CRASH_ID').alias('state_off_count')).orderBy(
        desc('state_off_count')).take(top_n_states)
    top_state_off_list = [row['VEH_LIC_STATE_ID'] for row in top_states_with_hig_offnces]
    intd_df2 = intd_df1.where(col('VEH_LIC_STATE_ID').isin(top_state_off_list))
    wspec = Window.orderBy(desc('veh_make_count'))
    intd_df3 = intd_df2.groupBy('VEH_MAKE_ID').agg(countDistinct('CRASH_ID').alias('veh_make_count')).orderBy(
        desc('veh_make_count'))
    intd_df4 = intd_df3.withColumn('rank', dense_rank().over(wspec))
    top_veh_make = intd_df4.filter(intd_df4['rank'] <= top_n_veh_make).collect()
    vehicle_list = [row['VEH_MAKE_ID'] for row in top_veh_make]
    return list(enumerate(vehicle_list, start=1))


df_charges, df_damages, df_units, df_endorse, df_prim_person, df_restrict = create_tables(folder_path)

count_crashes_male = num_crashes_gender_accidents(df_prim_person, prop['ANALYSIS_1_VAR_1'], prop['ANALYSIS_1_VAR_2'])
helper.write_output(prop, count_crashes_male, 'analysis_1')

count_two_wheelers = num_two_whlrs_bkd_for_crash(df_units, eval(prop['ANALYSIS_2']))
helper.write_output(prop, count_two_wheelers, 'analysis_2')

state = state_with_highest_accidents_gender(df_prim_person, prop['ANALYSIS_3'])
helper.write_output(prop, state, 'analysis_3')

veh_list = veh_make_highest_injury(df_units, prop['ANALYSIS_4_VAR_1'], prop['ANALYSIS_4_VAR_2'])
helper.write_output(prop, veh_list, 'analysis_4')

final_df = top_ethnic_group_veh_bdy_style(df_units, df_prim_person)
helper.write_output(prop, final_df, 'analysis_5')

top_zip_list = top_zip_crashes_alcohol(df_units, df_prim_person, prop['ANALYSIS_6'])
helper.write_output(prop, top_zip_list, 'analysis_6')

crash_count = count_crash_id_no_damage(df_damages, df_units)
helper.write_output(prop, crash_count, 'analysis_7')

veh_make_list = analysis_eight(df_units, df_prim_person,
                               int(prop['ANALYSIS_8_VAR_1']), int(prop['ANALYSIS_8_VAR_2']),
                               int(prop['ANALYSIS_8_VAR_3']))
helper.write_output(prop, veh_make_list, 'analysis_8')
