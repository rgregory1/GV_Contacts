import pandas as pd
import numpy as np
from pathlib import Path


def process_contacts(file_name, current):
    print(f"processing {file_name}")

    # read in file to be processed
    df = pd.read_csv(
        current / "incoming_files" / file_name, dtype=str, header=None, sep="\t"
    )

    # rename all columns
    df.rename(
        columns={
            0: "STUDENT_ID",
            1: "CONTACT_ID",
            2: "RELATIONSHIP",
            3: "HAS_CUSTODY",
            4: "NAME_FIRST",
            5: "NAME_LAST",
            6: "HOME_PHONE",
            7: "WORK_PHONE",
            8: "CELL_PHONE",
            9: "EMAIL",
            10: "ON_MAILING_LIST",
            11: "IS_STUDENTS_RESIDENCE",
            12: "RESIDENCE_ADDRESS1",
            13: "RESIDENCE_ADDRESS2",
            14: "RESIDENCE_CITY",
            15: "RESIDENCE_STATE",
            16: "RESIDENCE_ZIPCODE",
            17: "MAIL_ADDRESS1",
            18: "MAIL_ADDRESS2",
            19: "MAIL_CITY",
            20: "MAIL_STATE",
            21: "MAIL_ZIPCODE",
            22: "EMERGENCY_CONTACT",
            23: "STATUS",
        },
        inplace=True,
    )

    # process some columns into Y/N
    df["HAS_CUSTODY"].replace("[Custody]", "Y", inplace=True)
    df["ON_MAILING_LIST"].replace("[Mail]", "Y", inplace=True)
    df["ON_MAILING_LIST"].replace("[No Mail]", "N", inplace=True)
    df["IS_STUDENTS_RESIDENCE"].replace("[Lives With]", "Y", inplace=True)
    df["IS_STUDENTS_RESIDENCE"].replace("[Doesn't Live With]", "N", inplace=True)
    df["EMERGENCY_CONTACT"].replace("[Non Emerg]", "N", inplace=True)
    df["EMERGENCY_CONTACT"].replace("[Emerg]", "Y", inplace=True)
    df["STATUS"].replace("[Active]", "Y", inplace=True)

    # remove partial address errors
    df["RESIDENCE_STATE"].replace("Not Set", "", inplace=True)
    df["MAIL_STATE"].replace("Not Set", "", inplace=True)

    # split workphone column for extensions
    df[["WORK_PHONE", "WORK_PHONE_EXTENSION"]] = df["WORK_PHONE"].str.split(
        "x", expand=True
    )

    # insert blank columns
    df.insert(4, "NAME_TITLE", np.nan)
    df.insert(6, "NAME_MIDDLE", np.nan)
    df.insert(8, "NAME_SUFFIX", np.nan)

    # insert work_phone_extension column in appropriate place
    cols = df.columns.tolist()
    cols.insert(11, cols.pop(cols.index("WORK_PHONE_EXTENSION")))
    df = df.reindex(columns=cols)

    def f(x):
        """check for value in another column, return Y or N depending"""
        if pd.isnull(x):
            return "Y"
        else:
            return "N"

    # create new column based on presense of value in mail_address1 column
    df["USE_RESIDENCE_FOR_MAILING"] = df.MAIL_ADDRESS1.apply(f)

    # insert newly created column
    cols = df.columns.tolist()
    cols.insert(21, cols.pop(cols.index("USE_RESIDENCE_FOR_MAILING")))
    df = df.reindex(columns=cols)

    # insert more blank columns
    df.insert(24, "MAIL_POBOX", np.nan)
    df.insert(28, "EDUCATIONAL_RIGHTS", np.nan)
    df.insert(30, "CONTACT_TYPE", np.nan)

    # drop unused student identifier columns
    df = df.drop([24, 25, 26], axis=1)

    # drop empty lines if no contact filled in
    df = df[df["CONTACT_ID"].notna()]

    ################## this section processes the relationships column

    # gather all relationships in column
    all_rels = df.RELATIONSHIP.unique().tolist()

    # exclude nan values from list
    all_rels = [x for x in all_rels if type(x) == str]
    all_rels

    # these relationships get a numveric value
    special_rels = [
        "Mother",
        "Father",
        "Step-Mother",
        "Step-Father",
        "DCF",
        "Educational Surrogate",
        "Foster Parent",
        "Legal Guardian",
        "Medical Professional",
        "Grandfather",
        "Grandmother",
    ]

    # resulting final_rels will be all relationships that are removed from file
    final_rels = [x for x in all_rels if x not in special_rels]
    final_rels
    df["RELATIONSHIP"].replace(final_rels, np.NaN, inplace=True)

    # add in numeric relationship values
    df["RELATIONSHIP"].replace(
        ["Mother", "Father", "Step-Mother", "Step-Father"], "1", inplace=True
    )
    df["RELATIONSHIP"].replace("DCF", "3", inplace=True)
    df["RELATIONSHIP"].replace("Educational Surrogate", "4", inplace=True)
    df["RELATIONSHIP"].replace("Foster Parent", "5", inplace=True)
    df["RELATIONSHIP"].replace("Legal Guardian", "6", inplace=True)
    df["RELATIONSHIP"].replace("Medical Professional", "5000", inplace=True)
    df["RELATIONSHIP"].replace(["Grandfather", "Grandmother"], "23", inplace=True)

    # df.to_csv(
    #     "std_contact.txt", index=False, header=False, sep="\t", line_terminator="\n"
    # )

    return df


def produce_data(file_name_1, file_name_2, current):
    df1 = process_contacts(file_name_1, current)
    df2 = process_contacts(file_name_2, current)

    frames = [df1, df2]
    result = pd.concat(frames)

    result.to_csv(
        current / "processed_files" / "std_contact.txt",
        index=False,
        header=False,
        sep="\t",
        line_terminator="\n",
    )

    """program to convert modern newlines to MS-DOS style newlines"""

    filename = current / "processed_files" / "std_contact.txt"
    with open(filename, "rb") as f:
        data = f.read()
    data = data.replace(b"\n", b"\r\n")
    with open(filename, "wb") as f:
        f.write(data)
