# -*- coding: utf-8 -*-

#
# Webhook client for receiving a payload from Gogs to process the job
#

from __future__ import print_function

# Python libraries
import os
import tempfile
import boto3
import requests
import json

# Functions from Python libraries
from shutil import copyfile
from glob import glob
from datetime import datetime

# Functions from unfoldingWord libraries
from general_tools.file_utils import unzip, add_file_to_zip, make_dir, write_file
from general_tools.url_utils import download_file

MAXJSON    = 10000
debugLevel = 6

def handle(event, context):
    myLog( "info", "Enter Handle")

    # Getting data from payload which is the JSON that was sent from tx-manager
    if 'data' not in event:
        raise Exception('"data" not in payload')
    data = event['data']

    env_vars = {}
    if 'vars' in event and isinstance(event['vars'], dict):
        env_vars = event['vars']

    commit_id = data['after']
    commit = None
    for commit in data['commits']:
        if commit['id'] == commit_id:
            break

    commit_url = commit['url']
    commit_message = commit['message']

    if 'https://git.door43.org/' not in commit_url and 'http://test.door43.org:3000/' not in commit_url:
        raise Exception('Currently only git.door43.org repositories are supported.')

    pre_convert_bucket = env_vars['pre_convert_bucket']
    cdn_bucket = env_vars['cdn_bucket']
    gogs_user_token = env_vars['gogs_user_token']
    api_url = env_vars['api_url']

    repo_name = data['repository']['name']
    repo_owner = data['repository']['owner']['username']
    compare_url = data['compare_url']

    if 'pusher' in data:
        pusher = data['pusher']
    else:
        pusher = {'username': commit['author']['username']}
    pusher_username = pusher['username']

    try:  # template of things to do based on repo
        tmp = open("template.json", "r")
        tmpRaw = tmp.read(MAXJSON)
        tmp.close()
        templates = json.loads(tmpRaw)
        myLog("detail", "templates: " + tmpRaw)
    except:
        myLog("error", "Cannot read transform template")
        return(2)

    # The following sections of code will:
    # 1) download and unzip repo files
    # 2) massage the repo files by creating a new directory and file structure
    # 3) zip up the massages filed
    # 4) upload massaged files to S3 in zip file

    # 1) Download and unzip the repo files
    repo_zip_url = commit_url.replace('commit', 'archive') + '.zip'
    repo_zip_file = os.path.join(tempfile.gettempdir(), repo_zip_url.rpartition('/')[2])
    repo_dir = tempfile.mkdtemp(prefix='repo_')
    try:
        print('Downloading {0}...'.format(repo_zip_url), end=' ')
        if not os.path.isfile(repo_zip_file):
            download_file(repo_zip_url, repo_zip_file)
    finally:
        print('finished.')

    # Unzip the archive
    try:
        print('Unzipping {0}...'.format(repo_zip_file), end=' ')
        unzip(repo_zip_file, repo_dir)
    finally:
        print('finished.')




    myLog( "info", "should be ok to here" )




    # bms #2 rich #2 follows
    # try: # Find doctype in template then process per template
    if True:
        isFound = False
        myLog("info", "looking for docType: " + docType)

        for item in templates['templates']:
            myLog("detail", "trying: " + item['doctype'])

            if item['doctype'] == docType:
                myLog("detail", "found: " + item['doctype'])

                try:  # Apply qualifying tests
                    for test in item['tests']:
                        myLog("info", test)
                        # invoke test
                except:
                    myLog("warning", "  Cannot apply tests.")

                # try: # apply transforms from template
                if True:
                    fmt = manifest['type']['id']

                    for trans in item['transforms']:
                        frm = trans['from']
                        myLog("info", "fmt: " + fmt + " frm: " + frm)

                        if fmt == frm:
                            if item['agent'] == 'local':  # not using tx use files as is
                                myLog("detail", "  tool: " + trans['tool'])
                                myLog("detail", "  orgDir: " + orgDir + " current: " + os.getcwd())
                                os.chdir(orgDir)
                                myLog("info", "pwd: " + os.getcwd())
                                tool = "converters/" + trans['tool']
                                source = trans['to']
                                myLog("info", "to: " + trans['to'])
                                src = workDir + repoName
                                tgt = outDir + dest + source
                                cmd = " -s " + src + " -d " + tgt
                                myLog("info", "cmd: " + tool + " " + cmd)

                                # try:
                                if True:
                                    res = subprocess.check_output(
                                        ["python", tool, "-s", src, "-d", tgt],
                                        stderr=subprocess.STDOUT,
                                        shell=False)
                                    myLog("loops", 'tool result: ' + res)
                            else:
                                fun = trans['function']

                                if fun == 'flatten':
                                    tmpFileDir = tempfile.mktemp(prefix='files_')
                                    flatten(os.path.join(inDir, repoName), tmpFileDir)

                                    if trans['to'] == 'html':
                                        if trans['tool'] == 'tx':
                                            # txResp = myTx( awsid, tmpFileDir, preConvertBucket, pusher, api_url, payload )
                                            #     def myTx( aswid, massagedFilesDir, preConvertBucket, authorUsername, api_url, data ):
                                            # Zip up the massaged files
                                            zipFilename = awsid + '.zip'  # context.aws_request_id is a unique ID
                                            # for this lambda call, so using it to not conflict with other requests
                                            zipFile = os.path.join(tempfile.gettempdir(), zipFilename)
                                            myLog("info", "zipFile: " + zipFile)
                                            mdFiles = glob(os.path.join(tmpFileDir, '*.md'))
                                            print('Zipping files from {0} to {1}...'.format(tmpFileDir, zipFile),
                                                  end=' ')
                                            fileCount = 0

                                            for mdFile in mdFiles:
                                                add_file_to_zip(zipFile, mdFile, os.path.basename(mdFile))
                                                fileCount += 1

                                            print('finished zipping: ' + str(fileCount) + " files.")

                                            # 4) Upload zipped file to the S3 bucket (you may want to do some try/catch
                                            #    and give an error if fails back to Gogs)
                                            print('Uploading {0} to {1} in {2}...'.format(zipFile, preConvertBucket,
                                                                                          zipFilename), end=' ')
                                            s3Client = boto3.client('s3')
                                            s3Client.upload_file(zipFile, preConvertBucket, zipFilename)
                                            print('finished upload.')
                                            # Send job request to tx-manager
                                            sourceUrl = 'https://s3-us-west-2.amazonaws.com/' + preConvertBucket + '/' + zipFilename  # we use us-west-2 for our s3 buckets
                                            txManagerJobUrl = api_url + '/tx/job'
                                            gogsUserToken = payload['secret']

                                            txPayload = {
                                                "user_token": gogsUserToken,
                                                "username": pusher,
                                                "resource_type": "obs",
                                                "input_format": "md",
                                                "output_format": "html",
                                                "source": sourceUrl,
                                            }

                                            headers = {"content-type": "application/json"}
                                            print('Making request to tx-Manager URL {0} with payload:'.format(
                                                txManagerJobUrl))
                                            print(txPayload)
                                            response = requests.post(txManagerJobUrl, json=txPayload, headers=headers)
                                            print('finished tx-manager request.')

                                            # for testing
                                            print('tx-manager response:', end=" ")
                                            print(response)
                                            jsonData = json.loads(response.text)
                                            print('jsonData', end=" ")
                                            print(jsonData)
                                            # If there was an error, in order to trigger a 400 error in the API Gateway, we need to raise an
                                            # exception with the returned 'errorMessage' because the API Gateway needs to see 'Bad Request:' in the string
                                            if 'errorMessage' in jsonData:
                                                raise Exception(jsonData['errorMessage'])

                                            # Unzip ZIP file into the door43Bucket
                                            convertedZipUrl = jsonData['job']['output']
                                            convertedZipFile = os.path.join(tempfile.gettempdir(),
                                                                            convertedZipUrl.rpartition('/')[2])
                                            # ========================================

                                            try:
                                                print('Downloading converted file from: {0} to: {1} ...'.format(
                                                    convertedZipUrl, convertedZipFile), end=' ')
                                                download_file(convertedZipUrl, convertedZipFile)
                                            finally:
                                                print('finished download.')

                                                # Unzip the archive
                                            door43Dir = tempfile.mkdtemp(prefix='door43_')

                                            if True:
                                                # if os.path.exists( convertedZipFile ):
                                                try:
                                                    print('Unzipping {0}...'.format(convertedZipFile), end=' ')
                                                    unzip(convertedZipFile, door43Dir)
                                                finally:
                                                    print('finished unzip.')
                                                usr = 'u/' + payload['repository']['owner']['username']
                                                s3ProjectKey = os.path.join(usr, repoName, hash)
                                                print("s3ProjectKey: " + s3ProjectKey)
                                            else:
                                                print('Nothing downloaded')

                                            # Delete existing files in door43.org for this Project Key
                                            s3Resource = boto3.resource('s3')
                                            s3Bucket = s3Resource.Bucket(door43Bucket)

                                            for obj in s3Bucket.objects.filter(Prefix=s3ProjectKey):
                                                s3Resource.Object(s3Bucket.name, obj.key).delete()

                                                ## Upload all files to the door43 bucket with the key of <user>/<repo_name>/<commit> of the repo
                                                # for root, dirs, files in os.walk(door43Dir):
                                                #    for file in files:
                                                #        path = os.path.join(root, file)
                                                #        key = s3ProjectKey + path.replace(door43Dir, '')
                                                #        s3Client.upload_file(os.path.join(root, file), door43Bucket, key)

                                                # Make a manifest.json file with this repo and commit data for later processing
                                                # manifestFile = os.path.join(tempfile.gettempdir(), 'manifest.json')
                                                # write_file(manifestFile, json.dumps(data))
                                                # s3Client.upload_file(manifestFile, door43Bucket, s3ProjectKey+'/manifest.json')

                                                # Delete the zip files we made above (when we convert to using callbacks, this should be done in the callback)
                                                # Rich: commented out so we can see the file for testing
                                                ##s3Resource.Object(preConvertBucket, zipFilename).delete()

                                                # return something to Gogs response for the webhook. Right now we will just returning the tx-manager response
                                                # return( jsonData )

                                if fun == 'mv_md':
                                    mv_md(inDir)
                                    fmt = 'md'

                                    # except( OSError, e ):
                                    #    myLog( "warning", "Cannot run tool: " + tool + " " + \
                                    #        cmd + ". Error: " + e.strerror )
                # except:
                #    myLog( "warning", "  Cannot apply transforms" )

                isFound = True
                break

        if isFound == False:
            myLog("error", "Cannot find docType: " + docIdx)
            print("504")
            sys.exit(4)

    # except:
    #    myLog( "error", "No support for docType: " + docType )
    #    print( "505" )
    #    sys.exit( 5 )

    # 2) Massage the content to just be a directory of MD files in alphabetical order as they should be compiled together in the converter
    resource_type = None
    input_format = None
    source_dir = os.path.join(repo_dir, repo_name, 'content')
    output_dir = tempfile.mktemp(prefix='files_')
    manifest_filepath = os.path.join(repo_dir, repo_name, 'manifest.json')
    # Get info from manifest.json if there is one
    if os.path.isfile(manifest_filepath):
        with open(manifest_filepath) as f:
            manifest = json.load(f)
            if 'resource' in manifest and 'id' in manifest['resource']:
                resource_type = 'bible'
            elif 'source_translations' in manifest and 'resource_slug' in manifest['source_translations'][0]:
                resource_type = manifest['status'][0]['resource_slug']
            elif 'status' in manifest and 'source_translations' in manifest['status'] and 'resource_slug' in manifest['status']['source_translations'][0]:
                resource_type = manifest['status']['source_translations'][0]['resource_slug']
    if not resource_type:
        if '-' in repo_name:
            resource_type = repo_name.split('-')[:-1]
        elif '_' in repo_name:
            resource_type = repo_name.split('_')[:-1]
        else:
            resource_type = repo_name

    if resource_type == 'ulb' or resource_type == 'udb':
        resource_type = 'bible'

    # Handle Bible files
    if resource_type == 'bible':
        input_format = "usfm"
        files = glob(os.path.join(source_dir, '*.usfm'))
        make_dir(output_dir)
        print('Massaging content from {0} to {1}...'.format(source_dir, output_dir), end=' ')
        for filename in files:
            copyfile(filename, os.path.join(output_dir, os.path.basename(filename)))
        print('finished.')
    # Handle OBS files
    elif resource_type == 'obs':
        input_format = "md"
        source_dir = os.path.join(repo_dir, repo_name, 'content')
        files = glob(os.path.join(source_dir, '*.md'))
        output_dir = tempfile.mktemp(prefix='files_')
        make_dir(output_dir)
        print('Massaging content from {0} to {1}...'.format(source_dir, output_dir), end=' ')
        for filename in files:
            copyfile(filename, os.path.join(output_dir, os.path.basename(filename)))
        # Copy over front and back matter
        copyfile(os.path.join(source_dir, '_front', 'front-matter.md'),
                 os.path.join(output_dir, 'front-matter.md'))
        copyfile(os.path.join(source_dir, '_back', 'back-matter.md'),
                 os.path.join(output_dir, 'back-matter.md'))
        print('finished.')
    else:
        raise Exception("Bad Request: No resource type could be determined for repository {0}/{1}.".format(repo_owner, repo_name))

    # 3) Zip up the massaged files
    zip_filename = context.aws_request_id+'.zip' # context.aws_request_id is a unique ID for this lambda call, so using it to not conflict with other requests
    zip_filepath = os.path.join(tempfile.gettempdir(), zip_filename)
    files = glob(os.path.join(output_dir, '*'))
    print('Zipping files from {0} to {1}...'.format(output_dir, zip_filepath), end=' ')
    for filename in files:
        add_file_to_zip(zip_filepath, filename, os.path.basename(filename))
    if os.path.isfile(manifest_filepath):
        add_file_to_zip(zip_filepath, manifest_filepath, os.path.basename(manifest_filepath))
    print('finished.')

    # 4) Upload zipped file to the S3 bucket (you may want to do some try/catch and give an error if fails back to Gogs)
    print('Uploading {0} to {1}...'.format(zip_filepath, pre_convert_bucket), end=' ')
    s3_client = boto3.client('s3')
    file_key = "tx-webhook-client/"+zip_filename
    s3_client.upload_file(zip_filepath, pre_convert_bucket, file_key)
    print('finished.')

    # Send job request to tx-manager
    source_url = 'https://s3-us-west-2.amazonaws.com/{0}/{1}'.format(pre_convert_bucket, file_key)  # we use us-west-2 for our s3 buckets
    tx_manager_job_url = api_url+'/tx/job'
    identifier = "{0}/{1}/{2}".format(repo_owner, repo_name, commit_id[:10])  # The way to know which repo/commit goes to this job request
    payload = {
        "identifier": identifier,
        "user_token": gogs_user_token,
        "resource_type": resource_type,
        "input_format": input_format,
        "output_format": "html",
        "source": source_url,
        "callback": api_url+'/client/callback'
    }
    headers = {"content-type": "application/json"}

    print('Making request to tx-Manager URL {0} with payload:'.format(tx_manager_job_url))
    print(payload)
    print('...', end=' ')
    response = requests.post(tx_manager_job_url, json=payload, headers=headers)
    print('finished.')

    # for testing
    print('tx-manager response:')
    print(response)

    if not response:
        raise Exception('Bad request: unable to convert')

    if 'errorMessage' in response:
        raise Exception('Bad request: '.format(response['errorMessage']))

    json_data = json.loads(response.text)
    print("json:")
    print(json_data)

    if 'job' not in json_data:
        raise Exception('Bad request: Did not receive a response from the tX Manager')

    build_log_json = {
        'job_id': json_data['job']['job_id'],
        'repo_name': repo_name,
        'repo_owner': repo_owner,
        'commit_id': commit_id,
        'committed_by': pusher_username,
        'commit_url': commit_url,
        'compare_url': compare_url,
        'commit_message': commit_message,
        'created_at': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        'eta': json_data['job']['eta'],
        'resource_type': resource_type,
        'input_format': input_format,
        'output_format': 'html',
        'success': None,
        'status': 'started',
        'message': 'Conversion in progress...'
    }

    if 'errorMessage' in json_data:
        build_log_json['status'] = 'failed'
        build_log_json['message'] = json_data['errorMessage']

    # Upload files to S3:

    # S3 location vars
    s3_commit_key = 'u/{0}'.format(identifier)
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(cdn_bucket)

    # Remove everything in the bucket with the s3_commit_key prefix so old files are removed, if any
    for obj in bucket.objects.filter(Prefix=s3_commit_key):
        s3_resource.Object(bucket.name, obj.key).delete()

    # Make a build_log.json file with this repo and commit data for later processing, upload to S3
    build_log_file = os.path.join(tempfile.gettempdir(), 'build_log_request.json')
    write_file(build_log_file, build_log_json)
    bucket.upload_file(build_log_file, s3_commit_key+'/build_log.json', ExtraArgs={'ContentType': 'application/json', 'CacheControl': 'max-age=0'})
    print('Uploaded the following content from {0} to {1}/build_log.json'.format(build_log_file, s3_commit_key))
    print(build_log_json)

    # Upload the manifest.json file to the cdn_bucket if it exists
    if os.path.isfile(manifest_filepath):
        bucket.upload_file(manifest_filepath, s3_commit_key+'/manifest.json', ExtraArgs={'ContentType': 'application/json', 'CacheControl': 'max-age=0'})
        print('Uploaded the manifest.json file to {0}/manifest.json'.format(s3_commit_key))

    # If there was an error, in order to trigger a 400 error in the API Gateway, we need to raise an
    # exception with the returned 'errorMessage' because the API Gateway needs to see 'Bad Request:' in the string
    if 'errorMessage' in json_data:
        raise Exception(json_data['errorMessage'])

    return build_log_json



def myLog( level, msg): # very simple local debug
    sw = {
      "debug"   : 6,      "loops"   : 5,
      "detail"  : 4,      "info"    : 3,
      "warning" : 2,      "error"   : 1
    }

    l = sw.get( level, 5 )

    if l <= debugLevel:
        print( ( "  " * l ) + level + " " + " " * ( 7 - len( level )) + msg )


def ifCopy( frm, too ):
  # copy frm to too if frm exists

    if os.path.exists( frm ):
        copyfile( frm, too )


def flatten(dir, mfd):
    # pull chunks up to chapter level
    # dir is like: /tmp/input/<user>/<repo>/
    # content is under dir [<chapter>]<chunk>.[md|txt]
    myLog('info', "flatten: " + dir + " mfd: " + mfd)
    os.chdir(dir)
    content = os.path.join(dir, "content")

    if os.path.exists(content):
        os.chdir(content)

    mdFiles = glob('*/*.md')
    make_dir(mfd)
    print("mdFiles")
    print(mdFiles)
    fileCount = 0

    for mdFile in mdFiles:
        newFile = mdFile.replace('/', '-')
        os.rename(mdFile, os.path.join(mfd, newFile))
        myLog("debug", "mdFile: " + mdFile + " newFile: " + newFile)
        fileCount += 1

    myLog("info", "mdFiles: " + str(fileCount))
    # want front matter to be before 01.md and back matter to be after 50.md
    ifCopy(os.path.join(dir, '_front', 'front-matter.md'),
           os.path.join(mfd, '00_front-matter.md'))
    ifCopy(os.path.join(dir, '_back', 'back-matter.md'),
           os.path.join(mfd, '51_back-matter.md'))


def mv_md(dir):
    # change txt files from dir to have .md extensions
    myLog('info', "mv_md dir: " + dir)

    for root, dirs, files in os.walk(dir):
        myLog("detail", "root: " + root)
        c = 1

        if debugLevel > 5:
            for nme in files:
                srcPath = join(root, nme)
                myLog("debug", "srcPath: " + srcPath)

        for nme in files:
            srcPath = join(root, nme)
            myLog("details", "srcPath: " + srcPath)

            if srcPath.find(".txt") > -1:
                dst = srcPath[:srcPath.rfind('.')]
                myLog("debug", "dst:     " + dst + '.md')
                os.rename(srcPath, dst + '.md')

        if debugLevel > 5:
            for nme in files:
                srcPath = join(root, nme)
                myLog("debug", "srcPath: " + srcPath)

