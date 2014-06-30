import boto
import sys
import os
import pprint
from subprocess import call
import pandas as pd
import os
import numpy as np




AWS_BUCKET_KEY   = 'au.com.andretrosky.roll'
RHI_PUB_KEY_NAME = '73487FA275BBE4142E3DCFD53C95E1C17B86447D.asc'
RHI_UID          = 'Rhiannon Butcher <rhiannon@protodata.com.au>'
DRE_PUB_KEY_NAME = '0x93FEF9BB.asc'
DRE_UID          = 'andre trosky <andretrosky@gmail.com>'
TARBALL_FILENAME = 'VEC-spatial-join-and-targets-reduced.tar.gz'
ENCRYPTED_VEC    = 'VEC-spatial-join-and-targets-reduced.gpg'
REDUCED_ROLL_HEADER = ["VEC ID", "MB_CODE11", "MB_CAT11", "SA1_MAIN11", "SA2_MAIN11", "SA2_NAME11", "SA3_CODE11", "SA3_NAME11", "SA4_CODE11","SA4_NAME11", "STE_CODE11", "STE_NAME11", "GCC_CODE11", "GCC_NAME11", "VIC_LH_DISTRICT", "VIC_UH_REGION", "FED_ELECT", "TARGET", "CAMP_TARGET"]


def getRollFiles(conn):
    print 'in getAwsKeys'
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    print 'keys in %s bucket:' % AWS_BUCKET_KEY

    rollFileNames = []

    for yar in daBucket.list():
        if yar.name.startswith('results1/') and yar.name.endswith('.csv'):
            pprint.pprint(yar.name)
            #strip out results1/ prefix for filename to save to
            daBucket.get_key(yar.name).get_contents_to_filename(yar.name[9:])
            rollFileNames.append(yar.name[9:])

    return rollFileNames
  

def getRhiPubKey(conn):
    print 'in getRhiPubKey'
    # bootstrap used to download
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    daBucket.get_key(RHI_PUB_KEY_NAME).get_contents_to_filename(RHI_PUB_KEY_NAME)
    cmd = ["gpg", "--import", RHI_PUB_KEY_NAME]
    call(cmd)



def getDrePubKey(conn):
    print 'in getDrePubKey'
    # bootstrap used to download
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    daBucket.get_key(DRE_PUB_KEY_NAME).get_contents_to_filename(DRE_PUB_KEY_NAME)
    cmd = ["gpg", "--import", DRE_PUB_KEY_NAME]
    call(cmd)



def reduceFileCols(rollFileNames):
    reducedFileNames = []

    for rollFile in rollFileNames:
        print 'matching target vals to roll file: %s' % rollFile

        reduced_roll = []
        roll_csv = pd.read_csv(rollFile)
        roll_iterator = roll_csv.iterrows()

        for j, row in roll_iterator:
            row_dict = {}
                
            row_dic['VEC ID']          = row['VEC ID']
            row_dic['MB_CODE11']       = row['MB_CODE11']
            row_dic['MB_CAT11']        = row['MB_CAT11']
            row_dic['SA1_MAIN11']      = row['SA1_MAIN11']
            row_dic['SA2_MAIN11']      = row['SA2_MAIN11']
            row_dic['SA2_NAME11']      = row['SA2_NAME11']
            row_dic['SA3_CODE11']      = row['SA3_CODE11']
            row_dic['SA3_NAME11']      = row['SA3_NAME11']
            row_dic['SA4_CODE11']      = row['SA4_CODE11']
            row_dic['SA4_NAME11']      = row['SA4_NAME11']
            row_dic['STE_CODE11']      = row['STE_CODE11']
            row_dic['STE_NAME11']      = row['STE_NAME11']
            row_dic['GCC_CODE11']      = row['GCC_CODE11']
            row_dic['GCC_NAME11']      = row['GCC_NAME11']
            row_dic['VIC_LH_DISTRICT'] = row['VIC_LH_DISTRICT']
            row_dic['VIC_UH_REGION']   = row['VIC_UH_REGION']
            row_dic['FED_ELECT']       = row['FED_ELECT']
            row_dic['TARGET']          = row['TARGET']
            row_dic['CAMP_TARGET']     = row['CAMP_TARGET']

            reduced_roll.append(row_dict) 
            
        print 'saving this reduced roll to disk...'
        save_location = "%s_reduced.csv" % (rollFile[:len(rollFile)-4])
        reduced_roll_df = pd.DataFrame(reduced_roll)
        reduced_roll_df.to_csv(save_location, header=REDUCED_ROLL_HEADER, index=False)
        print 'SUCCESS: saved %s to disk', save_location 

        reducedFileNames.append(save_location)

    return reducedFileNames



def bundleFiles(rollFileNames):
    print 'in bundleFiles'

    cmd = ["tar", "czf", TARBALL_FILENAME]
    for f in rollFileNames:
        cmd.append(f)
    
    call(cmd)



def encryptTarBall(UID):
    print 'encrypting VEC tarball...'
    cmd = ["gpg", "-o", ENCRYPTED_VEC, "--encrypt", "-r", UID , TARBALL_FILENAME]

    call(cmd)



def seeYaLaterTarball(conn):
    print 'kicking a tarball goal at s3 stadium'
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    k = boto.s3.key.Key(daBucket, ENCRYPTED_VEC)
    k.key = ENCRYPTED_VEC

    try:
        k.set_contents_from_filename(ENCRYPTED_VEC)
        print 'SUCCESS: uploaded %s to S3' % ENCRYPTED_VEC 
    except Exception:
        print 'ERROR: could no upload %s to S3' % ENCRYPTED_VEC




if __name__ == '__main__':
    print 'bundler welcomes you'
    print 'connecting to S3...'
    conn = boto.connect_s3()
    print '...connected to S3'

    rollFileNames = getRollFiles(conn)
    getRhiPubKey(conn)
    getDrePubKey(conn)
    reducedFileNames = reduceFileCols(rollFileNames)
    bundleFiles(reducedFileNames)
    encryptTarBall(DRE_UID)
    #encryptTarBall(RHI_UID)
    seeYaLaterTarball(conn)
