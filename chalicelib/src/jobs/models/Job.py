from datetime import datetime
from chalicelib.src.database.methods import pgdb_connection
import chalicelib.src.database.constants as db_constants
import chalicelib.src.jobs.constants as job_constants
import chalicelib.src.general.helpers as helpers


class Job:
    def __init__(
        self,
        job_name: str,
        job_description: str,
        reporting_year: int,
        layer_id: int,
        created_by: int = None,
        misc_info: dict = None
    ):
        self.name = job_name
        self.description = job_description
        self.status = job_constants.STATUSES["IN_PROGRESS"]
        self.created_by = "NULL" if created_by is None else created_by
        self.reporting_year = reporting_year
        self.layer_id = layer_id
        self.misc_info = misc_info # will be added to the end of event post descriptions to help identify job
        cursor = pgdb_connection.cursor()

        # make sure the job is listed in job_list table of database
        cursor.execute(
            f"""SELECT job_id, created_date, CURRENT_TIMESTAMP as current_timestamp
                FROM {db_constants.DB_TABLES["JOB_LIST"]} 
                WHERE job_name = '{job_name}' AND job_description = '{job_description}'"""
        )
        result = cursor.fetchone()

        if result is not None: 
            self.id, self.created_date, self.start_time = result
            
        else: # new jobs only
            cursor.execute(
                f"""INSERT INTO {db_constants.DB_TABLES["JOB_LIST"]} (job_name, job_description, created_date, created_by)
                            VALUES ('{self.name}', '{self.description}',
                            CURRENT_TIMESTAMP, {self.created_by})
                RETURNING job_id, CURRENT_TIMESTAMP as created_date, CURRENT_TIMESTAMP as current_time"""
            )
            result = cursor.fetchone()
            self.id, self.created_date, self.start_time = result
        
        self.end_time = "NULL"
        self.last_updated_by = self.created_by
        # make sure the status of the job is listed in job_status,
        # is set to "IN PROGRESS",
        # and update timestamps.
        cursor.execute(
            f"""UPDATE {db_constants.DB_TABLES["JOB_STATUS"]}
                SET job_status = '{self.status}', job_start_time = '{self.start_time}', job_end_time = NULL, last_updated_date = '{self.start_time}', last_updated_by = {self.created_by}
                WHERE   job_id = {self.id} AND 
                        reporting_year = {self.reporting_year} AND 
                        layer_id = {self.layer_id}
                RETURNING job_status_id, created_date"""
        )
        result = cursor.fetchone()
        if result is None:
            cursor.execute(
                f"""INSERT INTO {db_constants.DB_TABLES["JOB_STATUS"]} (  job_id, reporting_year, layer_id, 
                                                                    job_status, job_start_time, job_end_time, 
                                                                    created_date, created_by, last_updated_date, 
                                                                    last_updated_by)
                            VALUES 	(	{self.id}, {self.reporting_year}, {self.layer_id}, 
                                        '{self.status}', '{self.start_time}', NULL, 
                                        CURRENT_TIMESTAMP, {self.created_by}, CURRENT_TIMESTAMP, 
                                        {self.last_updated_by})
                RETURNING job_status_id, CURRENT_TIMESTAMP"""
            )
            result = cursor.fetchone()
        self.status_id, self.job_status_created_date = result
        pgdb_connection.commit()

    def update_status(self, status_key_str: str):
        """used to report how a job ended (either success or failure)"""
        # check if status string is a valid status string
        if status_key_str not in job_constants.STATUSES.keys():
            raise ValueError(
                f"""{helpers.full_class_name(self)}.update_status: Invalid input status key, refer to constants.jobs"""
            )
        else:
            new_status = job_constants.STATUSES[status_key_str]

        # create the update value for the job_end_time column of the job_status table
        if new_status in [
            job_constants.STATUSES["COMPLETE"],
            job_constants.STATUSES["ERROR"],
        ]:
            update_end_time_string = ", job_end_time = CURRENT_TIMESTAMP"
        else:
            update_end_time_string = ""

        cursor = pgdb_connection.cursor()
        # always update the status and last_updated
        cursor.execute(
            f"""UPDATE {db_constants.DB_TABLES["JOB_STATUS"]} 
                SET job_status = '{new_status}', last_updated_date = CURRENT_TIMESTAMP {update_end_time_string}
                WHERE job_status_id = {self.status_id}
                RETURNING CURRENT_TIMESTAMP"""
        )
        result = cursor.fetchone()[0]
        self.status = new_status
        self.last_updated_date = result

        if new_status in [
            job_constants.STATUSES["COMPLETE"],
            job_constants.STATUSES["ERROR"],
        ]:
            self.end_time = result

        pgdb_connection.commit()

    def post_event(self, job_id_string: str, event_id_string: str, *event_details_info):
        """
        all job event constants are stored as dicts {name: str, details: str},
        named in the form: "<JOB_ID_STRING>_<EVENT_ID_STRING>_EVENT". refer to constants.jobs
        """
        try:
            event_info = getattr(
                job_constants,
                f"{job_id_string.upper()}_{event_id_string.upper()}_EVENT",
            )
        except AttributeError:
            raise ValueError(
                f"{helpers.full_class_name(self)}.post_event: Invalid job id string or event id string."
            )
        cursor = pgdb_connection.cursor()
        cursor.execute(
            f"""INSERT INTO {db_constants.DB_TABLES["JOB_EVENT"]} (job_status_id, event_name, event_details, created_date)
                VALUES ({self.status_id}, '{event_info["name"]}', '{event_info["details"].format(*event_details_info) + (" (" + ", ".join([f"{key}: {value}" for key, value in self.misc_info.items()]) + ")" if self.misc_info is not None else "")}', CURRENT_TIMESTAMP)
            """
        )
        pgdb_connection.commit()

    def get_runtime(self):
        """returns a string representing the runtime of the job in HH:MM:SS format.
        this cannot be run with the job is in progress"""
        if self.end_time is None:
            raise RuntimeError(
                f"{helpers.full_class_name(self)}.get_runtime: Error, there was an attempt to get the runtime of an unfinished job"
            )

        start = datetime.strptime(
            self.start_time.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"
        )  # Replace with your start timestamp value
        end = datetime.strptime(
            self.end_time.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"
        )  # Replace with your end timestamp value

        duration = end - start

        total_seconds = duration.total_seconds()

        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)

        formatted_duration = "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)

        return formatted_duration
