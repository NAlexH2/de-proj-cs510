import sys

from src.dataemailer import tar_data
from src.grabber import DataGrabber
from src.uploadgdrive import upload_to_gdrive
from src.utils import curr_time_micro
from src.publisher import PipelinePublisher


if __name__ == "__main__":
    if "-U" in sys.argv:
        print(
            f"{curr_time_micro()} Upload arg found. Will send to google drive "
            + "nharris@pdx.edu."
        )
    if "-P" in sys.argv:
        print(
            f"{curr_time_micro()} Publish arg found. Will push ALL entries in "
            + "EVERY RECORD to Google pub/sub."
        )
    if "-T" in sys.argv:
        print(f"{curr_time_micro()} Tar arg found. Will tarball each record.")

    pub_worker: PipelinePublisher = PipelinePublisher()
    data_collect = DataGrabber(pub_worker=pub_worker)
    data_collect.data_grabber_main()
    OK_size = data_collect.OK_response.size
    bad_size = data_collect.bad_response.size

    # TODO: gmail acc to email from to myself
    # data_emailer(ok_size=OK_size, bad_size=bad_size)

    if "-U" in sys.argv:
        start_time = curr_time_micro()
        upload_to_gdrive()
        print(
            f"{curr_time_micro()} Upload to google drive completed. "
            + f"Started at {start_time}."
        )

    if "-T" in sys.argv:
        tar_data()  # Just tar the file instead. For now.

    # Publish all the data that's been collected so far.
    if "-P" in sys.argv:
        start_time = curr_time_micro()
        pub_worker.publish_data()
        print(f"{curr_time_micro()} Publish started at {start_time}.")

    print(f"{curr_time_micro()} Operation finished.")
