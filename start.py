#reducer is much like bundler except that it reduces the number of attributes down
#to that defined in REDUCED_ROLL_HEADER. the reduced files are saved the the 
#hdd then bundled, then encrypted then sent to aws S3


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
REDUCED_ROLL_HEADER = ["VEC ID", "MB_CODE11", "MB_CAT11", "SA1_MAIN11", "SA2_MAIN11", 
                       "SA2_NAME11", "SA3_CODE11", "SA3_NAME11", "SA4_CODE11",
                       "SA4_NAME11", "STE_CODE11", "STE_NAME11", "GCC_CODE11", 
                       "GCC_NAME11", "VIC_LH_DISTRICT", "VIC_UH_REGION", "FED_ELECT", 
                       "TARGET", "CAMP_TARGET"]


def getRollFiles(conn):
    print 'in getAwsKeys'
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    print 'keys in %s bucket:' % AWS_BUCKET_KEY

    rollFileNames = []

    for yar in daBucket.list():
        if yar.name.startswith('results_full/') and yar.name.endswith('.csv'):
            pprint.pprint(yar.name)
            #strip out results_full/ prefix for filename to save to
            daBucket.get_key(yar.name).get_contents_to_filename(yar.name[13:])
            rollFileNames.append(yar.name[13:])
   
    assert len(rollFileNames) > 0, 'ASSERT ERROR: rollFileNames is empty'

    return rollFileNames
  

def getRhiPubKey(conn):
    print 'in getRhiPubKey'
    # bootstrap used to download
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    daBucket.get_key(RHI_PUB_KEY_NAME).get_contents_to_filename(RHI_PUB_KEY_NAME)

    assert os.path.exists(RHI_PUB_KEY_NAME) == True, 'ASSERT ERROR: rhi pub key no exists'

    cmd = ["gpg", "--import", RHI_PUB_KEY_NAME]

    try:
        call(cmd)
    except:
        print 'ERROR: subprocess error in getRhiPubKey'
        exit(1)



def getDrePubKey(conn):
    print 'in getDrePubKey'
    # bootstrap used to download
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    daBucket.get_key(DRE_PUB_KEY_NAME).get_contents_to_filename(DRE_PUB_KEY_NAME)

    assert os.path.exists(DRE_PUB_KEY_NAME) == True, 'ASSERT ERROR: dre pub key no exists'

    cmd = ["gpg", "--import", DRE_PUB_KEY_NAME]

    try:
        call(cmd)
    except:
        print 'ERROR: subprocess error in getDrePubKey'
        exit(1)



def reduceFileCols(rollFileNames):
    reducedFileNames = []

    #loop through the spatially joined files and do reducing for each one
    for rollFile in rollFileNames:
        print 'matching target vals to roll file: %s' % rollFile

        reduced_roll = []
        roll_csv = pd.read_csv(rollFile)
        assert roll_csv is not None, 'ASSERT ERROR:roll_csv df is None'

        roll_iterator = roll_csv.iterrows()
        assert roll_iterator is not None, 'ASSERT ERROR:roll_iterator is None'

        for j, row in roll_iterator:
            row_dict = {}
                
            row_dict['VEC ID']          = row['VEC ID']
            row_dict['MB_CODE11']       = row['MB_CODE11']
            row_dict['MB_CAT11']        = row['MB_CAT11']
            row_dict['SA1_MAIN11']      = row['SA1_MAIN11']
            row_dict['SA2_MAIN11']      = row['SA2_MAIN11']
            row_dict['SA2_NAME11']      = row['SA2_NAME11']
            row_dict['SA3_CODE11']      = row['SA3_CODE11']
            row_dict['SA3_NAME11']      = row['SA3_NAME11']
            row_dict['SA4_CODE11']      = row['SA4_CODE11']
            row_dict['SA4_NAME11']      = row['SA4_NAME11']
            row_dict['STE_CODE11']      = row['STE_CODE11']
            row_dict['STE_NAME11']      = row['STE_NAME11']
            row_dict['GCC_CODE11']      = row['GCC_CODE11']
            row_dict['GCC_NAME11']      = row['GCC_NAME11']
            row_dict['VIC_LH_DISTRICT'] = row['VIC_LH_DISTRICT']
            row_dict['VIC_UH_REGION']   = row['VIC_UH_REGION']
            row_dict['FED_ELECT']       = row['FED_ELECT']
            row_dict['TARGET']          = row['TARGET']
            row_dict['CAMP_TARGET']     = row['CAMP_TARGET']

            reduced_roll.append(row_dict) 
            
        print 'saving this reduced roll to disk...'
        save_location = "%s_reduced.csv" % (rollFile[:len(rollFile)-4])
        assert save_location is not None, 'ASSERT ERROR: save_location is len 0'
        reduced_roll_df = pd.DataFrame(reduced_roll)
        reduced_roll_df.to_csv(save_location, header=REDUCED_ROLL_HEADER, index=False)
        print 'SUCCESS: saved %s to disk', save_location 

        reducedFileNames.append(save_location)

    return reducedFileNames



def bundleFiles(reducedFileNames):
    print 'in bundleFiles'

    cmd = ["tar", "czf", TARBALL_FILENAME]
    for f in reducedFileNames:
        cmd.append(f)
    
    try:
        call(cmd)
    except:
        print 'SUBPROCESS ERROR:bundleFile problem with tarballing files'
        exit(1)


def encryptTarBall(UID):
    print 'encrypting VEC tarball...'
    cmd = ["gpg", "-o", ENCRYPTED_VEC, "--encrypt", "-r", UID , TARBALL_FILENAME]

    try:
        call(cmd)
    except:
        print 'SUBPROCESS ERROR:encryptTarBall problem with encrypting tarball'
        exit(1)



def seeYaLaterTarball(conn):
    print 'kicking a tarball goal at s3 stadium'
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    k = boto.s3.key.Key(daBucket, ENCRYPTED_VEC)
    k.key = ENCRYPTED_VEC
    assert k.key is not None, 'ASSERT ERROR: k.key is None'

    try:
        k.set_contents_from_filename(ENCRYPTED_VEC)
        print 'SUCCESS: uploaded %s to S3' % ENCRYPTED_VEC 
    except:
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
