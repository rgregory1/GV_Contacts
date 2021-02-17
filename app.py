import paramiko
from paramiko import sftp_client
import datetime
import credentials
from pathlib import Path

from process_contacts import process_contacts, produce_data

# get timestamp for log
temp_timestamp = str(datetime.datetime.now())
print(2 * "\n")
print(temp_timestamp)

current = Path.cwd()
print(current)

# setup connection


def grab_file(file_name, current):

    """ Get files from RaspberryPi """

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=credentials.pi_host,
        username=credentials.pi_user,
        password=credentials.pi_pass,
        port=22,
    )
    sftp_client = ssh.open_sftp()

    localpath = current / "incoming_files" / file_name
    remotepath = "/public/" + file_name

    files = sftp_client.listdir()
    print(files)

    sftp_client.get(remotepath, localpath)

    sftp_client.close()
    ssh.close()
    print(f"Retrieved {file_name} from remote")


grab_file("GV_contacts_2.txt", current)
grab_file("GV_contacts_1.txt", current)


produce_data("GV_contacts_1.txt", "GV_contacts_2.txt", current)


# Put files to GV server
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(
    hostname=credentials.gv_host,
    username=credentials.gv_user,
    password=credentials.gv_pass,
    look_for_keys=False,
)
sftp_client = ssh.open_sftp()


sftp_client.put("processed_files/std_contact.txt", "std_contact.txt")
print("Put file on remote server")
sftp_client.close()
ssh.close()