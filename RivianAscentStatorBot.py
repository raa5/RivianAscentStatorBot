########################################################################################
# Import libraries
########################################################################################
import pandas as pd
import os
import requests
import json
import schedule
import time
import pytz
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pyspark.sql import SparkSession
from databricks import sql


########################################################################################
# Databricks Configuration
########################################################################################
DATABRICKS_SERVER_HOSTNAME = "rivian-prod-us-west-2.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/b050a7573faba9ab"
DATABRICKS_ACCESS_TOKEN = os.getenv("DATABRICKS_ACCESS_TOKEN")

slack_token = os.getenv("SLACK_TOKEN")
url = os.getenv("URL")

########################################################################################
# Slack setup
########################################################################################
client = WebClient(token=slack_token)


########################################################################################
# Function To Send Message TO Slack
########################################################################################
def send_message_to_slack(channel, text):
    try:
        response = client.chat_postMessage(channel=channel, text=text)
        print(f"Message sent to {channel} with timestamp {response['ts']}")
    except SlackApiError as e:
        print(f"Error sending message to Slack: {e.response['error']}")


########################################################################################
# Function to Connect to Databricks
########################################################################################
def create_databricks_connection():
    return sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_ACCESS_TOKEN,
    )


########################################################################################
# Function to Execute Query and Get Results
########################################################################################
def execute_query(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0].upper() for desc in cursor.description]
        return pd.DataFrame(result, columns=columns)


########################################################################################
# Function defining all queries to run every hour
########################################################################################
def job():
    t0 = time.time()
    conn = create_databricks_connection()

    local_tz = pytz.timezone("America/Chicago")  # Change this to your expected timezone
    utc_now = datetime.now(pytz.utc)  # Get current UTC time
    local_now = utc_now.astimezone(local_tz)  # Convert to local timezone
    current_hour = local_now.hour
    current_time = local_now.strftime("%Y-%m-%d %H:00")

    one_hour_before = datetime.now() - timedelta(hours=1)
    recorded_at = one_hour_before.strftime("%Y-%m-%d %H:00")
    eight_hours_before = datetime.now() - timedelta(hours=8)
    recorded_at_summary = eight_hours_before.strftime("%Y-%m-%d %H:00")

    # Define the queries
    ########################################################################################
    # Query 20 - Every Hour
    ########################################################################################
    query_20 = f"""
    select count(distinct product_serial) as COUNT, STATION_NAME , work_location_desc as PARAMETER_NAME
    from manufacturing.mes.fct_work_location_jobs
    where shop_name = 'DU03'
    and line_name ilike '%STTR%'
    and station_name = '020'
    and started_at > '{recorded_at}'
    and job_status != 'OK'
    group by station_name, work_location_desc
    """
    ########################################################################################
    # Query 40 - Every Hour
    ########################################################################################
    query_40 = f"""
    SELECT COUNT(distinct product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
    FROM manufacturing.spinal.fct_spinal_parameter_records
    WHERE shop_name = 'DU03'
    AND line_name ilike '%STTR%'
    AND STATION_NAME ilike '%40%'
    AND PARAMETER_NAME = 'Force process value'
    AND parameter_id = 2
    AND overall_process_status = 'NOK'
    AND recorded_at > '{recorded_at}'
    GROUP BY STATION_NAME, PARAMETER_NAME
    """

    ########################################################################################
    # Query 50 - Every Hour
    ########################################################################################
    query_50 = f"""
    WITH alarm_data AS (
        SELECT *,
            LAG(cleared_at) OVER (PARTITION BY alarm_source_scada_short_name ORDER BY activated_at) AS prev_cleared_at
        FROM manufacturing.drive_unit.fct_du03_scada_alarms
        WHERE alarm_source_scada_short_name ILIKE '%STTR01-050%'
        AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
        AND alarm_priority_desc IN ('high', 'critical')
    )

    SELECT 
        COUNT(*) AS COUNT,
        '050' AS STATION_NAME,
        'Twisting Check Plate Fails' AS PARAMETER_NAME
    FROM alarm_data
    WHERE (activated_at > prev_cleared_at + INTERVAL '30 seconds' OR prev_cleared_at IS NULL)
    AND alarm_description ILIKE '%Assembly error%Task[301]%'

    UNION ALL

    SELECT 
        COUNT(*) AS COUNT,
        '050' AS STATION_NAME,
        TRIM(BOTH ' []' FROM SPLIT_PART(alarm_description, 'Key', 2)) AS PARAMETER_NAME
    FROM alarm_data
    WHERE alarm_description ILIKE '%Gripper%work%'
    GROUP BY parameter_name;
    """

    ########################################################################################
    # Query 70 - Every Hour
    ########################################################################################
    query_70 = f"""
    SELECT 
          COUNT(*) as COUNT,
          '070' as STATION_NAME,
          'Bad Cuts/Welding Fail' as ALARM_DESCRIPTION
    FROM manufacturing.drive_unit.fct_du03_scada_alarms
    WHERE alarm_source_scada_short_name ILIKE '%STTR01-070%'
    AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at}'
    AND alarm_priority_desc IN ('high', 'critical')
    AND alarm_description ILIKE '%Assembly error%'
    group by STATION_NAME
    """

    ########################################################################################
    # Query 90 - Every Hour
    ########################################################################################
    query_90 = f"""
    SELECT 
    COUNT(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
        FROM manufacturing.spinal.fct_spinal_parameter_records
        WHERE SHOP_NAME = 'DU03'
        AND line_name = 'STTR01'
        AND STATION_NAME = '090'
        -- AND overall_process_status = 'NOK'
        AND recorded_at > '{recorded_at}'
        AND (
            (PARAMETER_NAME = 'Value Height Pin X' AND (parameter_value_raw < 40 OR parameter_value_raw > 46.3)) OR
            (PARAMETER_NAME = 'Value Pixle Area Pin X' AND (parameter_value_raw < 2600 OR parameter_value_raw > 7500)) OR
            (PARAMETER_NAME = 'Value Blob X Feret Diameters Pin X' AND (parameter_value_raw < 1.8 OR parameter_value_raw > 3.6)) OR
            (PARAMETER_NAME = 'Value Blob Y Feret Diameters Pin X' AND (parameter_value_raw < 0.8 OR parameter_value_raw > 2.2)) OR
            (PARAMETER_NAME = 'Value Angle 1 Pin X' AND (parameter_value_raw < 13 OR parameter_value_raw > 45)) OR
            (PARAMETER_NAME = 'Value Angle 2 Pin X' AND (parameter_value_raw < -45 OR parameter_value_raw > 13)) OR
            (PARAMETER_NAME = 'Value Level Difference' AND (parameter_value_raw < 0 OR parameter_value_raw > 0.7)) OR
            (PARAMETER_NAME = 'Value Angle Connection Phase 1' AND (parameter_value_raw < -2.5 OR parameter_value_raw > 2.5)) OR
            (PARAMETER_NAME = 'Value Angle Connection Phase 2' AND (parameter_value_raw < -2.5 OR parameter_value_raw > 2.5)) OR
            (PARAMETER_NAME = 'Value Angle Connection Phase 3' AND (parameter_value_raw < -2.5 OR parameter_value_raw > 2.5)) OR
            (PARAMETER_NAME = 'Value Height Connection Phase 1' AND (parameter_value_raw < 11.35 OR parameter_value_raw > 12.90)) OR
            (PARAMETER_NAME = 'Value Height Connection Phase 2' AND (parameter_value_raw < 11.35 OR parameter_value_raw > 12.90)) OR
            (PARAMETER_NAME = 'Value Height Connection Phase 3' AND (parameter_value_raw < 11.35 OR parameter_value_raw > 12.90)) OR
            (PARAMETER_NAME = 'Value X Connection Element 1' AND (parameter_value_raw < -5.10 OR parameter_value_raw > -3.9)) OR
            (PARAMETER_NAME = 'Value X Connection Element 2' AND (parameter_value_raw < -6.30 OR parameter_value_raw > -5.6)) OR
            (PARAMETER_NAME = 'Value Y Connection Element 1' AND (parameter_value_raw < -23.85 OR parameter_value_raw > -22.95)) OR
            (PARAMETER_NAME = 'Value Y Connection Element 2' AND (parameter_value_raw < -94.95 OR parameter_value_raw > -94.05)) 
        )
        GROUP BY STATION_NAME, PARAMETER_NAME
        ORDER BY COUNT DESC
    """

    ########################################################################################
    # Query 100 - Every Hour
    ########################################################################################
    query_100 = f"""
     SELECT COUNT(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
        FROM manufacturing.spinal.fct_spinal_parameter_records
        WHERE SHOP_NAME = 'DU03'
        AND line_name = 'STTR01'
        AND STATION_NAME = '100'
        AND overall_process_status = 'NOK'
        AND recorded_at > '{recorded_at}'
        -- AND (
        --     ((PARAMETER_NAME = 'AmbientTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 50)) AND (work_location_id = 01 or work_location_id = 02)) OR
        --     ((PARAMETER_NAME = 'Area Waveform UV Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
        --     ((PARAMETER_NAME = 'Area Waveform VW Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
        --     ((PARAMETER_NAME = 'Area Waveform WU Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
        --     ((PARAMETER_NAME = 'Humidity Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_id = 02)) OR
        --     ((PARAMETER_NAME = 'BalanceOfAllPhasesU Value' AND (parameter_value_num < 0 OR parameter_value_num > 1.5)) AND (work_location_id = 01)) OR
        --     ((PARAMETER_NAME = 'Insulation Resistance UVW to GND Value' AND (parameter_value_num < 200 OR parameter_value_num > 5000)) AND (work_location_id = 01)) OR
        --     ((PARAMETER_NAME = 'Insulation Voltage UVW to GND Value' AND (parameter_value_num < 450 OR parameter_value_num > 550)) AND (work_location_id = 01)) OR
        --     ((PARAMETER_NAME = 'PartTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_id = 01)) OR
        --     ((PARAMETER_NAME = 'Pdiv HvAc Value' AND (parameter_value_num < 800 OR parameter_value_num > 10000)) AND (work_location_id = 01)) OR
        --     ((PARAMETER_NAME = 'Pdiv UV Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
        --     ((PARAMETER_NAME = 'Pdiv VW Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
        --     ((PARAMETER_NAME = 'Pdiv WU Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
        --     ((PARAMETER_NAME = 'PhaseResistance between UV Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
        --     ((PARAMETER_NAME = 'PhaseResistance between VW Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
        --     ((PARAMETER_NAME = 'PhaseResistance between WU Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
        --     ((PARAMETER_NAME = 'Withstand Current UVW to GND Value' AND (parameter_value_num < 0 OR parameter_value_num > 15)) AND (work_location_id = 01)) OR
        --     ((PARAMETER_NAME = 'Withstand Voltage UVW to GND Value' AND (parameter_value_num < 1850 OR parameter_value_num > 1950)) AND (work_location_id = 02)) 
        -- )
        GROUP BY STATION_NAME, PARAMETER_NAME
        ORDER BY COUNT DESC
    """

    ########################################################################################
    # Query 180 - Every Hour
    ########################################################################################
    query_180 = f"""
    SELECT COUNT(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
    FROM manufacturing.spinal.fct_spinal_parameter_records
    WHERE SHOP_NAME = 'DU03'
    AND line_name = 'STTR01'
    AND STATION_NAME = '180'
    AND overall_process_status = 'NOK'
    AND recorded_at > '{recorded_at}'
    AND (
         ((PARAMETER_NAME = 'AmbientTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 50)) AND (work_location_id = 01 or work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Area Waveform UV Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Area Waveform VW Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Area Waveform WU Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Humidity Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'InbalanceOfAllPhasesU Value' AND (parameter_value_num < 0 OR parameter_value_num > 1.5)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Insulation Resistance UVW to GND Value' AND (parameter_value_num < 200 OR parameter_value_num > 10000)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Insulation Voltage UVW to GND Value' AND (parameter_value_num < 450 OR parameter_value_num > 550)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'PartTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Pdiv HvAc Value' AND (parameter_value_num < 800 OR parameter_value_num > 10000)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Pdiv UV Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Pdiv VW Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'Pdiv WU Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
        ((PARAMETER_NAME = 'PhaseResistance between UV Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'PhaseResistance between VW Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'PhaseResistance between WU Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Withstand Current UVW to GND Value' AND (parameter_value_num < 0 OR parameter_value_num > 15)) AND (work_location_id = 01)) OR
        ((PARAMETER_NAME = 'Withstand Voltage UVW to GND Value' AND (parameter_value_num < 1850 OR parameter_value_num > 1950)) AND (work_location_id = 02)) 
    )
    GROUP BY STATION_NAME, PARAMETER_NAME
    ORDER BY COUNT DESC
    """

    ########################################################################################
    # Query 210 - Unique SN COUNT - Every Hour
    ########################################################################################
    query_210_unique_sn = f"""
    select count(distinct product_serial) as COUNT, station_name
    FROM manufacturing.spinal.fct_spinal_parameter_records
    WHERE line_name = 'STTR01'
    AND STATION_NAME = '210'
    AND overall_process_status = 'NOK'
    AND recorded_at > '{recorded_at}'
    AND (
        ((PARAMETER_NAME = 'AmbientTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 50)) AND (work_location_name= '01' or work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Area Waveform UV Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Area Waveform VW Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Area Waveform WU Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Humidity Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'InbalanceOfAllPhasesU Value' AND (parameter_value_num < 0 OR parameter_value_num > 1.5)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Insulation Resistance UVW to GND Value' AND (parameter_value_num < 200 OR parameter_value_num > 10000)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Insulation Voltage UVW to GND Value' AND (parameter_value_num < 450 OR parameter_value_num > 550)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'PartTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Pdiv HvAc Value' AND (parameter_value_num < 800 OR parameter_value_num > 10000)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Pdiv UV Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Pdiv VW Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'Pdiv WU Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_name= '02')) OR
        ((PARAMETER_NAME = 'PhaseResistance between UV Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'PhaseResistance between VW Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'PhaseResistance between WU Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Withstand Current UVW to GND Value' AND (parameter_value_num < 0 OR parameter_value_num > 15)) AND (work_location_name= '01')) OR
        ((PARAMETER_NAME = 'Withstand Voltage UVW to GND Value' AND (parameter_value_num < 1850 OR parameter_value_num > 1950)) AND (work_location_name= '02'))
    )
    GROUP BY ALL
    """

    ########################################################################################
    # Query 40 - Fails by Hairpin Origin - Every Hour
    ########################################################################################
    query_40_hairpin_origin = f"""
    with

    nest_parameter_records as 
        (
        select product_serial, station_name, parameter_name, parameter_value_raw, overall_process_status, recorded_at, parameter_id, overall_process_status
        -- from manufacturing.mes.fct_parameter_records
        from manufacturing.spinal.fct_spinal_parameter_records
        where 
            shop_name = 'DU03'
            and line_name = 'STTR01'
            and station_name like '%30%'
            and parameter_name = 'Nest'
        ),

    genealogy_hist as 
        (
        select product_serial, scanned_child_serial, consumed_at, product_part_desc, child_part_desc, scanned_child_data
        from manufacturing.mes.fct_genealogy_hist
        where
            shop_name = 'DU03'
            and line_name = 'STTR01'
        ),

    stack_serial as 
        (
        select scanned_child_serial, product_serial
        from manufacturing.mes.fct_genealogy_hist
        where line_name = 'STTR01'
        -- and scanned_child_part in ('PT00237854-C') 
        ),

    wire_spool as 
        (
        select product_serial, product_part, parameter_name, parameter_value_raw, recorded_at
        -- from manufacturing.mes.fct_parameter_records
        from manufacturing.spinal.fct_spinal_parameter_records
        where
            shop_name = 'DU03'
            and line_name = 'STTR01'
            and station_name like '%30%'
            and parameter_name ilike '%batch%'
            -- and parameter_value_raw ilike '%PT00649019-C%' 
        ),

    op_forty as
        (
        select product_serial, station_name, recorded_at, result_status, parameter_id, overall_process_status, parameter_name
        -- from manufacturing.mes.fct_parameter_records
        from manufacturing.spinal.fct_spinal_parameter_records
        where
            line_name = 'STTR01'
            and station_name ilike '%40%'
            and overall_process_status = 'NOK'
            and parameter_name = 'Force process value'

        )

    select distinct
        -- SS.scanned_child_serial as Stack_Serial,
        -- WS.parameter_value_raw as Copper_Wire_Spool,
        -- -- NPR.product_serial as Nest_Product_Serial,
        count(*) AS COUNT,
        opf.station_name as STATION_NAME,
        NPR.station_name as Sttr_030_Hairpin_Origin
        -- GH.product_serial as Stator_Assembly_Serial_Number,
        -- opf.result_status as Sttr_040_Result_Status,
        -- opf.recorded_at as Sttr_040_Recorded_At_Central_Time,
        -- substring (WS.parameter_value_raw, position('C' in ws.parameter_value_raw) + 1, 8) as Copper_Wire_8_Digit

    from nest_parameter_records as NPR

    join genealogy_hist as GH
        on NPR.product_serial = GH.scanned_child_serial
    join op_forty as opf
        on GH.product_serial = opf.product_serial
    join stack_serial as SS
        ON GH.product_serial = SS.product_serial
    left join wire_spool as WS
        on NPR.product_serial = WS.product_serial

    WHERE
        opf.station_name ILIKE '%40%'
        and opf.overall_process_status = 'NOK'
        and opf.recorded_at > '{recorded_at}'
        and opf.parameter_id = 2
        AND opf.PARAMETER_NAME = 'Force process value'
        group by all
    """

    ########################################################################################
    # Query 50 - Fails by Hairpin Origin - Every Hour
    #######################################################################################
    query_50_hairpin_origin = f"""
    with

    nest_parameter_records as 
        (
        select product_serial, station_name, parameter_name, parameter_value_raw, overall_process_status, recorded_at
        -- from manufacturing.mes.fct_parameter_records
        from manufacturing.spinal.fct_spinal_parameter_records
        where 
            shop_name = 'DU03'
            and line_name = 'STTR01'
            and station_name like '030%'
            and parameter_name = 'Nest'
        ),

    genealogy_hist as 
        (
        select product_serial, scanned_child_serial, consumed_at, product_part_desc, child_part_desc, scanned_child_data
        from manufacturing.mes.fct_genealogy_hist
        where
            shop_name = 'DU03'
            and line_name = 'STTR01'
        ),

    stack_serial as 
        (
        select scanned_child_serial, product_serial
        from manufacturing.mes.fct_genealogy_hist
        where line_name = 'STTR01'
        -- and scanned_child_part in ('PT00237854-C') 
        ),

    wire_spool as 
        (
        select product_serial, product_part, parameter_name, parameter_value_raw, recorded_at
        -- from manufacturing.mes.fct_parameter_records
        from manufacturing.spinal.fct_spinal_parameter_records
        where
            shop_name = 'DU03'
            and line_name = 'STTR01'
            and station_name like '030%'
            and parameter_name ilike '%batch%'
            -- and parameter_value_raw ilike '%PT00237846-C%' 
        ),

    op_fifty as
        (
        select product_serial, station_name, recorded_at, result_status
        -- from manufacturing.mes.fct_parameter_records
        from manufacturing.spinal.fct_spinal_parameter_records
        where
            line_name = 'STTR01'
            and station_name ilike '%050%'

        )

    select distinct
        -- SS.scanned_child_serial as Stack_Serial,
        -- WS.parameter_value_raw as Copper_Wire_Spool,
        -- NPR.product_serial as Nest_Product_Serial,
        count(distinct GH.product_serial) as COUNT,
        opf.station_name as STATION_NAME,
        NPR.station_name as Sttr_030_Hairpin_Origin
        -- GH.product_serial as Stator_Assembly_Serial_Number,
        -- opf.result_status as Sttr_050_Result_Status,
        -- opf.recorded_at as Sttr_050_Recorded_At_Central_Time,
        -- substring (WS.parameter_value_raw, position('C' in ws.parameter_value_raw) + 1, 8) as Copper_Wire_8_Digit

    from nest_parameter_records as NPR

    join genealogy_hist as GH
        on NPR.product_serial = GH.scanned_child_serial
    join op_fifty as opf
        on GH.product_serial = opf.product_serial
    join stack_serial as SS
        ON GH.product_serial = SS.product_serial
    left join wire_spool as WS
        on NPR.product_serial = WS.product_serial

    WHERE
        opf.station_name ILIKE '%050%'
        and opf.result_status = 'FAIL'
        and opf.recorded_at > '{recorded_at}'
        group by opf.station_name, NPR.station_name
    """

    ########################################################################################
    # Query 90 - Fails by Hairpin Origin - Every Hour
    ########################################################################################
    query_90_hairpin_origin = f"""
    with

    nest_parameter_records as 
        (
        select product_serial, station_name, parameter_name, parameter_value_raw, overall_process_status, recorded_at
        -- from manufacturing.mes.fct_parameter_records
        from manufacturing.spinal.fct_spinal_parameter_records
        where 
            shop_name = 'DU03'
            and line_name = 'STTR01'
            and station_name like '030%'
            and parameter_name = 'Nest'
        ),

    genealogy_hist as 
        (
        select product_serial, scanned_child_serial, consumed_at, product_part_desc, child_part_desc, scanned_child_data
        from manufacturing.mes.fct_genealogy_hist
        where
            shop_name = 'DU03'
            and line_name = 'STTR01'
        ),

    stack_serial as 
        (
        select scanned_child_serial, product_serial
        from manufacturing.mes.fct_genealogy_hist
        where line_name = 'STTR01'
        -- and scanned_child_part in ('PT00237854-C') 
        ),

    wire_spool as 
        (
        select product_serial, product_part, parameter_name, parameter_value_raw, recorded_at
        -- from manufacturing.mes.fct_parameter_records
        from manufacturing.spinal.fct_spinal_parameter_records
        where
            shop_name = 'DU03'
            and line_name = 'STTR01'
            and station_name like '030%'
            and parameter_name ilike '%batch%'
            -- and parameter_value_raw ilike '%PT00237846-C%' 
        ),

    op_sixty_five as
        (
        SELECT 
            DISTINCT product_serial, STATION_NAME, PARAMETER_NAME, recorded_at
            FROM manufacturing.spinal.fct_spinal_parameter_records
            WHERE line_name = 'STTR01'
            AND STATION_NAME = '090'
            -- AND overall_process_status = 'NOK'
            AND recorded_at > '{recorded_at}'
            AND (
                (PARAMETER_NAME = 'Value Height Pin X' AND (parameter_value_raw < 39 OR parameter_value_raw > 47)) OR
                (PARAMETER_NAME = 'Value Pixle Area Pin X' AND (parameter_value_raw < 5000 OR parameter_value_raw > 12000)) OR
                (PARAMETER_NAME = 'Value Blob X Feret Diameters Pin X' AND (parameter_value_raw < 2.6 OR parameter_value_raw > 3.9)) OR
                (PARAMETER_NAME = 'Value Blob Y Feret Diameters Pin X' AND (parameter_value_raw < 1.2 OR parameter_value_raw > 3.0)) OR
                (PARAMETER_NAME = 'Value Angle 1 Pin X' AND (parameter_value_raw < -45 OR parameter_value_raw > 45)) OR
                (PARAMETER_NAME = 'Value Angle 2 Pin X' AND (parameter_value_raw < -45 OR parameter_value_raw > 45)) OR
                (PARAMETER_NAME = 'Value Level Difference' AND (parameter_value_raw < 0 OR parameter_value_raw > 0.6)) OR
                (PARAMETER_NAME = 'Value Pin 1 edge to stack edge' AND (parameter_value_raw < 0 OR parameter_value_raw > 100000)) OR
                (PARAMETER_NAME = 'Value Pin 5 edge to stack edge' AND (parameter_value_raw < 0 OR parameter_value_raw > 100000))
            )
            -- GROUP BY STATION_NAME, PARAMETER_NAME
            -- ORDER BY COUNT DESC

        ),

    STTR_065_WEs AS 
        (
        SELECT *
        FROM main.adhoc.sttr_065_hmi_hairpin_naming_work_elements
        )

    select distinct
        -- SS.scanned_child_serial as Stack_Serial,
        -- WS.parameter_value_raw as Copper_Wire_Spool,
        -- NPR.product_serial as Nest_Product_Serial,
        count(distinct GH.product_serial) as COUNT,
        opsf.station_name as STATION_NAME,
        NPR.station_name as Sttr_030_Hairpin_Origin
        -- GH.product_serial as Stator_Assembly_Serial_Number
        -- opsf.result_status as Sttr_065_Result_Status,
        -- opsf.recorded_at as Sttr_065_Recorded_At_Central_Time,
        -- substring (WS.parameter_value_raw, position('C' in ws.parameter_value_raw) + 1, 8) as Copper_Wire_8_Digit,
        -- sfwe.Hairpins_In_Welded_Pin_Pair,
            -- LEFT(sfwe.Hairpins_In_Welded_Pin_Pair, 5) 
            -- || ' & ' || 
            -- SUBSTRING(sfwe.Hairpins_In_Welded_Pin_Pair, POSITION('&' IN sfwe.Hairpins_In_Welded_Pin_Pair) + 2, 5) 
            -- AS Hairpin_Short_Name

    from nest_parameter_records as NPR

    join genealogy_hist as GH
        on NPR.product_serial = GH.scanned_child_serial
    join op_sixty_five as opsf
        on GH.product_serial = opsf.product_serial
    join stack_serial as SS
        ON GH.product_serial = SS.product_serial
    left join wire_spool as WS
        on NPR.product_serial = WS.product_serial
    -- join STTR_065_WEs sfwe
    --     on opsf.work_element = sfwe.DELMIA_WE_Name

    WHERE
        opsf.station_name ILIKE '%090%'
        and opsf.recorded_at > '{recorded_at}'
        group by opsf.station_name, NPR.station_name
    """

#########################################################################################
# If Statement for Summary Queries at EOS
#########################################################################################
    if (15 <= current_hour < 16) or (5 <= current_hour < 6):
        # Define the queries
        ########################################################################################
        # Query 20 - Summary
        ########################################################################################
        query_20_summary = f"""
        select count(distinct product_serial) as COUNT, STATION_NAME , work_location_desc as PARAMETER_NAME
        from manufacturing.mes.fct_work_location_jobs
        where shop_name = 'DU02'
        and line_name = 'STTR01'
        and station_name = '020'
        and started_at > '{recorded_at_summary}'
        and work_location_name = '02'
        and job_status = 'NOK'
        group by station_name, work_location_desc
        """
        
        ########################################################################################
        # Query 40 - Summary
        ########################################################################################
        query_40_summary = f"""
        SELECT COUNT(distinct product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
        FROM manufacturing.spinal.fct_spinal_parameter_records
        WHERE line_name = 'STTR01'
        AND STATION_NAME = '040'
        AND PARAMETER_NAME = 'Force process value'
        AND parameter_id = 2
        AND overall_process_status = 'NOK'
        AND recorded_at > '{recorded_at_summary}'
        GROUP BY STATION_NAME, PARAMETER_NAME
        """

        ########################################################################################
        # Query 50 - Summary
        ########################################################################################
        query_50_summary = f"""
        WITH alarm_data AS (
            SELECT *,
                LAG(cleared_at) OVER (PARTITION BY alarm_source_scada_short_name ORDER BY activated_at) AS prev_cleared_at
            FROM manufacturing.drive_unit.fct_du02_scada_alarms
            WHERE alarm_source_scada_short_name ILIKE '%STTR01-050%'
            AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at_summary}'
            AND alarm_priority_desc IN ('high', 'critical')
        )

        SELECT 
            COUNT(*) AS COUNT,
            '050' AS STATION_NAME,
            'Twisting Check Plate Fails' AS PARAMETER_NAME
        FROM alarm_data
        WHERE (activated_at > prev_cleared_at + INTERVAL '30 seconds' OR prev_cleared_at IS NULL)
        AND alarm_description ILIKE '%Assembly error%Task[301]%'

        UNION ALL

        SELECT 
            COUNT(*) AS COUNT,
            '050' AS STATION_NAME,
            TRIM(BOTH ' []' FROM SPLIT_PART(alarm_description, 'Key', 2)) AS PARAMETER_NAME
        FROM alarm_data
        WHERE alarm_description ILIKE '%Gripper%work%'
        GROUP BY parameter_name;
        """

        ########################################################################################
        # Query 70 - Summary
        ########################################################################################
        query_70_summary = f"""
        SELECT 
            COUNT(*) as COUNT,
            '070' as STATION_NAME,
            'Bad Cuts/Welding Fail' as ALARM_DESCRIPTION
        FROM manufacturing.drive_unit.fct_du03_scada_alarms
        WHERE alarm_source_scada_short_name ILIKE '%STTR01-070%'
        AND CONVERT_TIMEZONE('UTC', 'America/Chicago', activated_at) > '{recorded_at_summary}'
        AND alarm_priority_desc IN ('high', 'critical')
        AND alarm_description ILIKE '%Assembly error%'
        group by STATION_NAME
        """

        ########################################################################################
        # Query 90 - Summary
        ########################################################################################
        query_90_summary = f"""
        SELECT 
        COUNT(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
            FROM manufacturing.spinal.fct_spinal_parameter_records
            WHERE SHOP_NAME = 'DU03'
            AND line_name = 'STTR01'
            AND STATION_NAME = '065'
            -- AND overall_process_status = 'NOK'
            AND recorded_at > '{recorded_at_summary}'
            AND (
                (PARAMETER_NAME = 'Value Height Pin X' AND (parameter_value_raw < 40 OR parameter_value_raw > 46.3)) OR
                (PARAMETER_NAME = 'Value Pixle Area Pin X' AND (parameter_value_raw < 2600 OR parameter_value_raw > 7500)) OR
                (PARAMETER_NAME = 'Value Blob X Feret Diameters Pin X' AND (parameter_value_raw < 1.8 OR parameter_value_raw > 3.6)) OR
                (PARAMETER_NAME = 'Value Blob Y Feret Diameters Pin X' AND (parameter_value_raw < 0.8 OR parameter_value_raw > 2.2)) OR
                (PARAMETER_NAME = 'Value Angle 1 Pin X' AND (parameter_value_raw < 13 OR parameter_value_raw > 45)) OR
                (PARAMETER_NAME = 'Value Angle 2 Pin X' AND (parameter_value_raw < -45 OR parameter_value_raw > 13)) OR
                (PARAMETER_NAME = 'Value Level Difference' AND (parameter_value_raw < 0 OR parameter_value_raw > 0.7)) OR
                (PARAMETER_NAME = 'Value Angle Connection Phase 1' AND (parameter_value_raw < -2.5 OR parameter_value_raw > 2.5)) OR
                (PARAMETER_NAME = 'Value Angle Connection Phase 2' AND (parameter_value_raw < -2.5 OR parameter_value_raw > 2.5)) OR
                (PARAMETER_NAME = 'Value Angle Connection Phase 3' AND (parameter_value_raw < -2.5 OR parameter_value_raw > 2.5)) OR
                (PARAMETER_NAME = 'Value Height Connection Phase 1' AND (parameter_value_raw < -2.5 OR parameter_value_raw > 2.5)) OR
                (PARAMETER_NAME = 'Value Height Connection Phase 2' AND (parameter_value_raw < -2.5 OR parameter_value_raw > 2.5)) OR
                (PARAMETER_NAME = 'Value Height Connection Phase 3' AND (parameter_value_raw < -2.5 OR parameter_value_raw > 2.5)) OR
                (PARAMETER_NAME = 'Value X Connection Element 1' AND (parameter_value_raw < -5.1 OR parameter_value_raw > -3.9)) OR
                (PARAMETER_NAME = 'Value Y Connection Element 1' AND (parameter_value_raw < -23.85 OR parameter_value_raw > -22.95)) OR
                (PARAMETER_NAME = 'Value X Connection Element 2' AND (parameter_value_raw < -6.3 OR parameter_value_raw > -5.6)) OR
                (PARAMETER_NAME = 'Value Y Connection Element 2' AND (parameter_value_raw < -94.95 OR parameter_value_raw > -94.05))             
            )
            GROUP BY STATION_NAME, PARAMETER_NAME
            ORDER BY COUNT DESC
        """

        ########################################################################################
        # Query 100 - Summary
        ########################################################################################
        query_100_summary = f"""
        SELECT COUNT(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
            FROM manufacturing.spinal.fct_spinal_parameter_records
            WHERE SHOP_NAME = 'DU03'
            AND line_name = 'STTR01'
            AND STATION_NAME = '100'
            AND overall_process_status = 'NOK'
            AND recorded_at > '{recorded_at_summary}
            -- AND (
            --     ((PARAMETER_NAME = 'AmbientTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 50)) AND (work_location_id = 01 or work_location_id = 02)) OR
            --     ((PARAMETER_NAME = 'Area Waveform UV Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
            --     ((PARAMETER_NAME = 'Area Waveform VW Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
            --     ((PARAMETER_NAME = 'Area Waveform WU Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
            --     ((PARAMETER_NAME = 'Humidity Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_id = 02)) OR
            --     ((PARAMETER_NAME = 'InbalanceOfAllPhasesU Value' AND (parameter_value_num < 0 OR parameter_value_num > 1.5)) AND (work_location_id = 01)) OR
            --     ((PARAMETER_NAME = 'Insulation Resistance UVW to GND Value' AND (parameter_value_num < 200 OR parameter_value_num > 10000)) AND (work_location_id = 01)) OR
            --     ((PARAMETER_NAME = 'Insulation Voltage UVW to GND Value' AND (parameter_value_num < 450 OR parameter_value_num > 550)) AND (work_location_id = 01)) OR
            --     ((PARAMETER_NAME = 'PartTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_id = 01)) OR
            --     ((PARAMETER_NAME = 'Pdiv HvAc Value' AND (parameter_value_num < 800 OR parameter_value_num > 10000)) AND (work_location_id = 01)) OR
            --     ((PARAMETER_NAME = 'Pdiv UV Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
            --     ((PARAMETER_NAME = 'Pdiv VW Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
            --     ((PARAMETER_NAME = 'Pdiv WU Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
            --     ((PARAMETER_NAME = 'PhaseResistance between UV Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
            --     ((PARAMETER_NAME = 'PhaseResistance between VW Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
            --     ((PARAMETER_NAME = 'PhaseResistance between WU Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
            --     ((PARAMETER_NAME = 'Withstand Current UVW to GND Value' AND (parameter_value_num < 0 OR parameter_value_num > 15)) AND (work_location_id = 01)) OR
            --     ((PARAMETER_NAME = 'Withstand Voltage UVW to GND Value' AND (parameter_value_num < 1850 OR parameter_value_num > 1950)) AND (work_location_id = 02)) 
            -- )
            GROUP BY STATION_NAME, PARAMETER_NAME
            ORDER BY COUNT DESC
        """

        ########################################################################################
        # Query 180 - Summary
        ########################################################################################
        query_180_summary = f"""
        SELECT COUNT(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
        FROM manufacturing.spinal.fct_spinal_parameter_records
        WHERE SHOP_NAME = 'DU03'
        AND line_name = 'STTR01'
        AND STATION_NAME = '180'
        AND overall_process_status = 'NOK'
        AND recorded_at > '{recorded_at}'
        AND (
            ((PARAMETER_NAME = 'AmbientTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 50)) AND (work_location_id = 01 or work_location_id = 02)) OR
            ((PARAMETER_NAME = 'Area Waveform UV Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
            ((PARAMETER_NAME = 'Area Waveform VW Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
            ((PARAMETER_NAME = 'Area Waveform WU Value' AND (parameter_value_num < -3 OR parameter_value_num > 3)) AND (work_location_id = 02)) OR
            ((PARAMETER_NAME = 'Humidity Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_id = 02)) OR
            ((PARAMETER_NAME = 'InbalanceOfAllPhasesU Value' AND (parameter_value_num < 0 OR parameter_value_num > 1.5)) AND (work_location_id = 01)) OR
            ((PARAMETER_NAME = 'Insulation Resistance UVW to GND Value' AND (parameter_value_num < 200 OR parameter_value_num > 10000)) AND (work_location_id = 01)) OR
            ((PARAMETER_NAME = 'Insulation Voltage UVW to GND Value' AND (parameter_value_num < 450 OR parameter_value_num > 550)) AND (work_location_id = 01)) OR
            ((PARAMETER_NAME = 'PartTemperature Value' AND (parameter_value_num < 0 OR parameter_value_num > 100)) AND (work_location_id = 01)) OR
            ((PARAMETER_NAME = 'Pdiv HvAc Value' AND (parameter_value_num < 800 OR parameter_value_num > 10000)) AND (work_location_id = 01)) OR
            ((PARAMETER_NAME = 'Pdiv UV Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
            ((PARAMETER_NAME = 'Pdiv VW Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
            ((PARAMETER_NAME = 'Pdiv WU Value' AND (parameter_value_num < 1400 OR parameter_value_num > 10000)) AND (work_location_id = 02)) OR
            ((PARAMETER_NAME = 'PhaseResistance between UV Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
            ((PARAMETER_NAME = 'PhaseResistance between VW Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
            ((PARAMETER_NAME = 'PhaseResistance between WU Value' AND (parameter_value_num < 10.637 OR parameter_value_num > 11.523)) AND (work_location_id = 01)) OR
            ((PARAMETER_NAME = 'Withstand Current UVW to GND Value' AND (parameter_value_num < 0 OR parameter_value_num > 15)) AND (work_location_id = 01)) OR
            ((PARAMETER_NAME = 'Withstand Voltage UVW to GND Value' AND (parameter_value_num < 1850 OR parameter_value_num > 1950)) AND (work_location_id = 02)) 
        )
        GROUP BY STATION_NAME, PARAMETER_NAME
        ORDER BY COUNT DESC
        """

        ########################################################################################
        # Query 40 - Fails by Hairpin Origin - Summary
        ########################################################################################
        query_40_hairpin_origin_summary = f"""
        with

        nest_parameter_records as 
            (
            select product_serial, station_name, parameter_name, parameter_value_raw, overall_process_status, recorded_at, parameter_id, overall_process_status
            -- from manufacturing.mes.fct_parameter_records
            from manufacturing.spinal.fct_spinal_parameter_records
            where 
                shop_name = 'DU03'
                and line_name = 'STTR01'
                and station_name ilike '%30%'
                and parameter_name = 'Nest'
            ),

        genealogy_hist as 
            (
            select product_serial, scanned_child_serial, consumed_at, product_part_desc, child_part_desc, scanned_child_data
            from manufacturing.mes.fct_genealogy_hist
            where
                shop_name = 'DU03'
                and line_name = 'STTR01'
            ),

        stack_serial as 
            (
            select scanned_child_serial, product_serial
            from manufacturing.mes.fct_genealogy_hist
            where line_name = 'STTR01'
            -- and scanned_child_part in ('PT00237854-C') 
            ),

        wire_spool as 
            (
            select product_serial, product_part, parameter_name, parameter_value_raw, recorded_at
            -- from manufacturing.mes.fct_parameter_records
            from manufacturing.spinal.fct_spinal_parameter_records
            where
                shop_name = 'DU03'
                and line_name = 'STTR01'
                and station_name like '%30%'
                and parameter_name ilike '%batch%'
                -- and parameter_value_raw ilike '%PT00649019-C%'
            ),

        op_forty as
            (
            select product_serial, station_name, recorded_at, result_status, parameter_id, overall_process_status, parameter_name
            -- from manufacturing.mes.fct_parameter_records
            from manufacturing.spinal.fct_spinal_parameter_records
            where
                line_name = 'STTR01'
                and station_name ilike '%40%'
                and overall_process_status = 'NOK'
                and parameter_name = 'Force process value'

            )

        select distinct
            -- SS.scanned_child_serial as Stack_Serial,
            -- WS.parameter_value_raw as Copper_Wire_Spool,
            -- -- NPR.product_serial as Nest_Product_Serial,
            count(*) AS COUNT,
            opf.station_name as STATION_NAME,
            NPR.station_name as Sttr_030_Hairpin_Origin
            -- GH.product_serial as Stator_Assembly_Serial_Number,
            -- opf.result_status as Sttr_040_Result_Status,
            -- opf.recorded_at as Sttr_040_Recorded_At_Central_Time,
            -- substring (WS.parameter_value_raw, position('C' in ws.parameter_value_raw) + 1, 8) as Copper_Wire_8_Digit

        from nest_parameter_records as NPR

        join genealogy_hist as GH
            on NPR.product_serial = GH.scanned_child_serial
        join op_forty as opf
            on GH.product_serial = opf.product_serial
        join stack_serial as SS
            ON GH.product_serial = SS.product_serial
        left join wire_spool as WS
            on NPR.product_serial = WS.product_serial

        WHERE
            opf.station_name ILIKE '%40%'
            and opf.overall_process_status = 'NOK'
            and opf.recorded_at > '{recorded_at_summary}'
            and opf.parameter_id = 2
            AND opf.PARAMETER_NAME = 'Force process value'
            group by all
        """

        ########################################################################################
        # Query 50 - Fails by Hairpin Origin - Summary
        #######################################################################################
        query_50_hairpin_origin_summary = f"""
        with

        nest_parameter_records as 
            (
            select product_serial, station_name, parameter_name, parameter_value_raw, overall_process_status, recorded_at
            -- from manufacturing.mes.fct_parameter_records
            from manufacturing.spinal.fct_spinal_parameter_records
            where 
                shop_name = 'DU03'
                and line_name = 'STTR01'
                and station_name like '030%'
                and parameter_name = 'Nest'
            ),

        genealogy_hist as 
            (
            select product_serial, scanned_child_serial, consumed_at, product_part_desc, child_part_desc, scanned_child_data
            from manufacturing.mes.fct_genealogy_hist
            where
                shop_name = 'DU03'
                and line_name = 'STTR01'
            ),

        stack_serial as 
            (
            select scanned_child_serial, product_serial
            from manufacturing.mes.fct_genealogy_hist
            where line_name = 'STTR01'
            -- and scanned_child_part in ('PT00237854-C') 
            ),

        wire_spool as 
            (
            select product_serial, product_part, parameter_name, parameter_value_raw, recorded_at
            -- from manufacturing.mes.fct_parameter_records
            from manufacturing.spinal.fct_spinal_parameter_records
            where
                shop_name = 'DU03'
                and line_name = 'STTR01'
                and station_name like '030%'
                and parameter_name ilike '%batch%'
                -- and parameter_value_raw ilike '%PT00237846-C%' 
            ),

        op_fifty as
            (
            select product_serial, station_name, recorded_at, result_status
            -- from manufacturing.mes.fct_parameter_records
            from manufacturing.spinal.fct_spinal_parameter_records
            where
                line_name = 'STTR01'
                and station_name ilike '%050%'

            )

        select distinct
            -- SS.scanned_child_serial as Stack_Serial,
            -- WS.parameter_value_raw as Copper_Wire_Spool,
            -- NPR.product_serial as Nest_Product_Serial,
            count(distinct GH.product_serial) as COUNT,
            opf.station_name as STATION_NAME,
            NPR.station_name as Sttr_030_Hairpin_Origin
            -- GH.product_serial as Stator_Assembly_Serial_Number,
            -- opf.result_status as Sttr_050_Result_Status,
            -- opf.recorded_at as Sttr_050_Recorded_At_Central_Time,
            -- substring (WS.parameter_value_raw, position('C' in ws.parameter_value_raw) + 1, 8) as Copper_Wire_8_Digit

        from nest_parameter_records as NPR

        join genealogy_hist as GH
            on NPR.product_serial = GH.scanned_child_serial
        join op_fifty as opf
            on GH.product_serial = opf.product_serial
        join stack_serial as SS
            ON GH.product_serial = SS.product_serial
        left join wire_spool as WS
            on NPR.product_serial = WS.product_serial

        WHERE
            opf.station_name ILIKE '%050%'
            and opf.result_status = 'FAIL'
            and opf.recorded_at > '{recorded_at_summary}'
            group by opf.station_name, NPR.station_name
        """

        ########################################################################################
        # Query 90 - Fails by Hairpin Origin - Summary
        ########################################################################################
        query_90_hairpin_origin_summary = f"""
        with

        nest_parameter_records as 
            (
            select product_serial, station_name, parameter_name, parameter_value_raw, overall_process_status, recorded_at
            -- from manufacturing.mes.fct_parameter_records
            from manufacturing.spinal.fct_spinal_parameter_records
            where 
                shop_name = 'DU03'
                and line_name = 'STTR01'
                and station_name like '030%'
                and parameter_name = 'Nest'
            ),

        genealogy_hist as 
            (
            select product_serial, scanned_child_serial, consumed_at, product_part_desc, child_part_desc, scanned_child_data
            from manufacturing.mes.fct_genealogy_hist
            where
                shop_name = 'DU03'
                and line_name = 'STTR01'
            ),

        stack_serial as 
            (
            select scanned_child_serial, product_serial
            from manufacturing.mes.fct_genealogy_hist
            where line_name = 'STTR01'
            -- and scanned_child_part in ('PT00237854-C') 
            ),

        wire_spool as 
            (
            select product_serial, product_part, parameter_name, parameter_value_raw, recorded_at
            -- from manufacturing.mes.fct_parameter_records
            from manufacturing.spinal.fct_spinal_parameter_records
            where
                shop_name = 'DU03'
                and line_name = 'STTR01'
                and station_name like '030%'
                and parameter_name ilike '%batch%'
                -- and parameter_value_raw ilike '%PT00237846-C%' 
            ),

        op_sixty_five as
            (
            SELECT DISTINCT product_serial, STATION_NAME, PARAMETER_NAME, recorded_at
            -- SELECT count(DISTINCT product_serial) as COUNT, STATION_NAME, PARAMETER_NAME
                FROM manufacturing.spinal.fct_spinal_parameter_records
                WHERE line_name = 'STTR01'
                AND STATION_NAME = '090'
                -- AND overall_process_status = 'NOK'
                -- AND recorded_at > '{recorded_at_summary}'
                AND (
                    (PARAMETER_NAME = 'Value Height Pin X' AND (parameter_value_raw < 39 OR parameter_value_raw > 47)) OR
                    (PARAMETER_NAME = 'Value Pixle Area Pin X' AND (parameter_value_raw < 5000 OR parameter_value_raw > 12000)) OR
                    (PARAMETER_NAME = 'Value Blob X Feret Diameters Pin X' AND (parameter_value_raw < 2.6 OR parameter_value_raw > 3.9)) OR
                    (PARAMETER_NAME = 'Value Blob Y Feret Diameters Pin X' AND (parameter_value_raw < 1.2 OR parameter_value_raw > 3.0)) OR
                    (PARAMETER_NAME = 'Value Angle 1 Pin X' AND (parameter_value_raw < -45 OR parameter_value_raw > 45)) OR
                    (PARAMETER_NAME = 'Value Angle 2 Pin X' AND (parameter_value_raw < -45 OR parameter_value_raw > -45)) OR
                    (PARAMETER_NAME = 'Value Level Difference' AND (parameter_value_raw < 0 OR parameter_value_raw > 0.6)) OR
                    (PARAMETER_NAME = 'Value Pin 1 edge to stack edge' AND (parameter_value_raw < 0 OR parameter_value_raw > 100000)) OR
                    (PARAMETER_NAME = 'Value Pin 5 edge to stack edge' AND (parameter_value_raw < 0 OR parameter_value_raw > 100000))
                )
                -- GROUP BY STATION_NAME, PARAMETER_NAME
                -- ORDER BY COUNT DESC

            ),

        STTR_065_WEs AS 
            (
            SELECT *
            FROM main.adhoc.sttr_065_hmi_hairpin_naming_work_elements
            )

        select distinct
            -- SS.scanned_child_serial as Stack_Serial,
            -- WS.parameter_value_raw as Copper_Wire_Spool,
            -- NPR.product_serial as Nest_Product_Serial,
            count(distinct GH.product_serial) as COUNT,
            opsf.station_name as STATION_NAME,
            NPR.station_name as Sttr_030_Hairpin_Origin
            -- GH.product_serial as Stator_Assembly_Serial_Number
            -- opsf.result_status as Sttr_065_Result_Status,
            -- opsf.recorded_at as Sttr_065_Recorded_At_Central_Time,
            -- substring (WS.parameter_value_raw, position('C' in ws.parameter_value_raw) + 1, 8) as Copper_Wire_8_Digit,
            -- sfwe.Hairpins_In_Welded_Pin_Pair,
                -- LEFT(sfwe.Hairpins_In_Welded_Pin_Pair, 5) 
                -- || ' & ' || 
                -- SUBSTRING(sfwe.Hairpins_In_Welded_Pin_Pair, POSITION('&' IN sfwe.Hairpins_In_Welded_Pin_Pair) + 2, 5) 
                -- AS Hairpin_Short_Name

        from nest_parameter_records as NPR

        join genealogy_hist as GH
            on NPR.product_serial = GH.scanned_child_serial
        join op_sixty_five as opsf
            on GH.product_serial = opsf.product_serial
        join stack_serial as SS
            ON GH.product_serial = SS.product_serial
        left join wire_spool as WS
            on NPR.product_serial = WS.product_serial
        -- join STTR_065_WEs sfwe
        --     on opsf.work_element = sfwe.DELMIA_WE_Name

        WHERE
            opsf.station_name ILIKE '%090%'
            and opsf.recorded_at > '{recorded_at_summary}'
            group by opsf.station_name, NPR.station_name
        """

        ########################################################################################
        # Execute summary queries and fetch data into DataFrames
        ########################################################################################
        df_20_summary = pd.read_sql(query_20_summary, conn)
        df_40_summary = pd.read_sql(query_40_summary, conn)
        df_50_summary = pd.read_sql(query_50_summary, conn)
        df_70_summary = pd.read_sql(query_70_summary, conn)
        df_90_summary = pd.read_sql(query_90_summary, conn)
        df_100_summary = pd.read_sql(query_100_summary, conn)
        df_180_summary = pd.read_sql(query_180_summary, conn)

        # df_210_unique_sn_summary = pd.read_sql(query_210_unique_sn_summary, conn)
        df_40_hairpin_origin_summary = pd.read_sql(query_40_hairpin_origin_summary, conn)
        df_50_hairpin_origin_summary = pd.read_sql(query_50_hairpin_origin_summary, conn)
        df_90_hairpin_origin_summary = pd.read_sql(query_90_hairpin_origin_summary, conn)
        
        ########################################################################################
        # Combine DataFrames
        ########################################################################################
        df_combined_summary = pd.concat(
            [
                df_20_summary,
                df_40_summary,
                df_50_summary,
                df_70_summary,
                df_90_summary,
                # df_80_summary,
                df_100_summary,
                df_180_summary,
                # df_210_summary,
            ],
            ignore_index=True,
        )

        df_combined_summary["PARAMETER_NAME"] = df_combined_summary[
            "ALARM_DESCRIPTION"
        ].fillna(df_combined_summary["PARAMETER_NAME"])
        df_combined_summary.drop(
            columns=["ALARM_DESCRIPTION"], inplace=True
        )  # Remove old column
        df_combined_summary = df_combined_summary[df_combined_summary["COUNT"] > 0]

        ########################################################################################
        # Sort combined DataFrame by 'COUNT' column
        ########################################################################################
        if "COUNT" in df_combined_summary.columns:
            df_combined_summary = df_combined_summary.sort_values(
                ["COUNT"], ascending=False, ignore_index=True
            )

        ########################################################################################
        # Aggregate total failures per station (without duplicates)
        ########################################################################################
        df_sum_summary = (
            df_combined_summary.groupby("STATION_NAME")["COUNT"].sum().reset_index()
        )

        ########################################################################################
        # Merge unique product serial failures for Station 210
        ########################################################################################
        if (
            not df_210_unique_sn_summary.empty
            and "STATION_NAME" in df_210_unique_sn_summary.columns
            and "COUNT" in df_210_unique_sn_summary.columns
        ):
            df_210_unique_sn_summary = df_210_unique_sn_summary.rename(
                columns={"COUNT": "FAIL_COUNT"}
            )

            # Merge Station 210's unique product serial failures into df_sum
            df_sum_summary = df_sum_summary.merge(
                df_210_unique_sn_summary, on="STATION_NAME", how="left"
            )

            # Replace total failure count with unique serial count for Station 210
            df_sum_summary["COUNT"] = df_sum_summary["FAIL_COUNT"].fillna(
                df_sum["COUNT"]
            )

            # Drop the temporary column
            df_sum_summary.drop(columns=["FAIL_COUNT"], inplace=True)
        else:
            print(
                "Warning: STATION_NAME or COUNT column missing from df_210_unique_sn. Falling back to regular sum."
            )

        ########################################################################################
        # Convert NaNs to 0 and ensure integer counts
        ########################################################################################
        df_sum_summary["COUNT"] = df_sum_summary["COUNT"].fillna(0).astype(int)

        ########################################################################################
        # Sort results
        ########################################################################################
        df_sum_summary = df_sum_summary[df_sum_summary["COUNT"] > 0]
        df_sum_summary = df_sum_summary.sort_values(
            ["COUNT"], ascending=False, ignore_index=True
        )

        ########################################################################################
        # Convert DataFrames to a JSON-like format (table-like string)
        ########################################################################################
        def df_to_table(df):
            table_str = df.to_string(index=False)
            return table_str

        df_combined_summary_str = df_to_table(df_combined_summary)
        df_sum_summary_str = df_to_table(df_sum_summary)
        df_hairpin_origin_summary = pd.concat(
            [
                df_40_hairpin_origin_summary,
                df_50_hairpin_origin_summary,
                df_90_hairpin_origin_summary,
            ],
            ignore_index=True,
        )
        
        df_hairpin_origin_summary["COUNT"] = pd.to_numeric(df_hairpin_origin_summary["COUNT"])
        df_hairpin_origin_summary["STATION_NAME"] = pd.to_numeric(df_hairpin_origin_summary["STATION_NAME"])
        df_hairpin_origin_summary = df_hairpin_origin_summary.sort_values(by=["COUNT", "STATION_NAME"], ascending=[False, True], ignore_index=True)
        
        df_hairpin_origin_summary_str = df_to_table(df_hairpin_origin_summary)

    ########################################################################################
    # Execute hourly queries and fetch data into DataFrames
    ########################################################################################
    df_20 = pd.read_sql(query_20, conn)
    df_40 = pd.read_sql(query_40, conn)
    df_50 = pd.read_sql(query_50, conn)
    df_70 = pd.read_sql(query_70, conn)
    df_90 = pd.read_sql(query_90, conn)
    df_100 = pd.read_sql(query_100, conn)
    df_180 = pd.read_sql(query_180, conn)
    # df_110 = pd.read_sql(query_110, conn)
    # df_210 = pd.read_sql(query_210, conn)

    df_210_unique_sn = pd.read_sql(query_210_unique_sn, conn)
    df_40_hairpin_origin = pd.read_sql(query_40_hairpin_origin, conn)
    df_50_hairpin_origin = pd.read_sql(query_50_hairpin_origin, conn)
    df_90_hairpin_origin = pd.read_sql(query_90_hairpin_origin, conn)

    ########################################################################################
    # Combine DataFrames
    ########################################################################################
    df_combined = pd.concat(
        [df_20, df_40, df_50, df_70, df_90, df_100, df_180],
        ignore_index=True,
    )

    df_combined["PARAMETER_NAME"] = df_combined["ALARM_DESCRIPTION"].fillna(
        df_combined["PARAMETER_NAME"]
    )
    df_combined.drop(columns=["ALARM_DESCRIPTION"], inplace=True)  # Remove old column
    df_combined = df_combined[df_combined["COUNT"] > 0]

    ########################################################################################
    # Sort combined DataFrame by 'COUNT' column
    ########################################################################################
    if "COUNT" in df_combined.columns:
        df_combined = df_combined.sort_values(
            ["COUNT"], ascending=False, ignore_index=True
        )

    ########################################################################################
    # Aggregate total failures per station (without duplicates)
    ########################################################################################
    df_sum = df_combined.groupby("STATION_NAME")["COUNT"].sum().reset_index()

    ########################################################################################
    # Merge unique product serial failures for Station 210
    ########################################################################################
    if (
        not df_210_unique_sn.empty
        and "STATION_NAME" in df_210_unique_sn.columns
        and "COUNT" in df_210_unique_sn.columns
    ):
        df_210_unique_sn = df_210_unique_sn.rename(columns={"COUNT": "FAIL_COUNT"})

        # Merge Station 210's unique product serial failures into df_sum
        df_sum = df_sum.merge(df_210_unique_sn, on="STATION_NAME", how="left")

        # Replace total failure count with unique serial count for Station 210
        df_sum["COUNT"] = df_sum["FAIL_COUNT"].fillna(df_sum["COUNT"])

        # Drop the temporary column
        df_sum.drop(columns=["FAIL_COUNT"], inplace=True)
    else:
        print(
            "Warning: STATION_NAME or COUNT column missing from df_210_unique_sn. Falling back to regular sum."
        )

    ########################################################################################
    # Convert NaNs to 0 and ensure integer counts
    ########################################################################################
    df_sum["COUNT"] = df_sum["COUNT"].fillna(0).astype(int)

    ########################################################################################
    # Sort results
    ########################################################################################
    df_sum = df_sum[df_sum["COUNT"] > 0]
    df_sum = df_sum.sort_values(["COUNT"], ascending=False, ignore_index=True)

    ########################################################################################
    # Convert DataFrames to a JSON-like format (table-like string)
    ########################################################################################
    def df_to_table(df):
        table_str = df.to_string(index=False)
        return table_str

    df_combined_str = df_to_table(df_combined)
    df_sum_str = df_to_table(df_sum)
    df_hairpin_origin = pd.concat(
        [df_40_hairpin_origin, df_50_hairpin_origin, df_90_hairpin_origin],
        ignore_index=True
    )
    df_hairpin_origin = df_hairpin_origin.sort_values(["COUNT"], ascending=False, ignore_index=True)
    df_hairpin_origin_str = df_to_table(df_hairpin_origin)

    ########################################################################################
    # Payload with both DataFrames formatted as tables
    ########################################################################################
    payload = {
        "blocks": [
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn", 
                    "text": f"*🚨Fail count by Parameter:* {recorded_at} to {(one_hour_before + timedelta(hours=1)).strftime('%H:00')}"
                },
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "```" + df_combined_str + "```",},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Fails by Station Pareto:*"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "```" + df_sum_str + "```",},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Fails by Hairpin Station:*"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "```" + df_hairpin_origin_str + "```",},
            },
            {"type": "divider"},
        ]
    }


    if (15 <= current_hour < 16) or (5 <= current_hour < 6):
        payload["blocks"].extend(
            [
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*🚨 Shift Summary (Last Shift)*",
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn", "text": "*Fail count by Parameter:* "
                        + recorded_at_summary
                        + " to "
                        + current_time,
                        # + (recorded_at_summary + timedelta(hours=200)).strftime("%Y-%m-%d %H:00"),
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "```" + df_to_table(df_combined_summary) + "```",
                    },
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*Fails by Station Pareto:*"},
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "```" + df_to_table(df_sum_summary) + "```",
                    },
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*Fails by Hairpin Station:*"},
                    },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "```" + df_hairpin_origin_summary_str + "```",
                    },
                },
                {"type": "divider"},  # Add a divider to separate sections clearly
            ]
        )

    ########################################################################################
    # Send the payload to Slack using a webhook
    ########################################################################################
    headers = {"Content-type": "application/json"}
    print(f"DEBUG: Sending message to Slack. Token: {slack_token}, Webhook URL: {url}")
    print(f"DATABRICKS_ACCESS_TOKEN Loaded: {DATABRICKS_ACCESS_TOKEN is not None}")
    print(f"SLACK_TOKEN Loaded: {slack_token is not None}")
    print(f"SLACK_WEBHOOK_URL Loaded: {url is not None}")

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        print(f"Slack API Error: {response.status_code} - {response.text}")
    else:
        print("Message successfully sent to Slack")
        
    print("Slack Payload:", json.dumps(payload, indent=2))




########################################################################################
# RUN job()
########################################################################################
job()  # Run the function once
