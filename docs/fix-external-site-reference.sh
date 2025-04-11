#!/usr/bin/env bash
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
EXT_DIR=external
EXT_URL_REF_FILE=/tmp/ext_ref_list.$$
if [ -d target ]
then
    cd target
    [ -d ${EXT_DIR} ] && rm -rf ${EXT_DIR}
    grep 'https://' *.html | grep -v '.apache.org' | grep '="https://' | awk -F'"' '{ print $2 }' | sort -u > ${EXT_URL_REF_FILE}
    for url in `cat ${EXT_URL_REF_FILE}`
    do
        newname=`echo $url | sed -e "s@https://@${EXT_DIR}/@"`
        dn=`dirname ${newname}`
        fn=`basename ${newname}`
        ext=`echo ${fn} | awk -F"." '{ if (NF > 1) { print $NF } }'`
        if [ ! -z "${ext}" ]
        then
            #echo "+ mkdir -p ${dn}"
            mkdir -p ${dn}
            #echo "+ curl -o ${newname} ${url}"
            curl -s -o ${newname} ${url}
            #echo "$url => ./${newname}"
            for f in *.html
            do
                cat ${f} | sed -e "s@${url}@./${newname}@g" > ${f}.$$ 
                #echo "+ diff ${f}.$$ ${f}"
                #echo "+ ===================="
                #diff ${f}.$$ ${f}
                #echo "+ ===================="
                mv ${f}.$$ ${f}
            done
        fi
    done
    rm -f ${EXT_URL_REF_FILE}
else
    echo "ERROR: Unable to locate the target folder - Run 'mvn site' command before kicking off this script"
    exit 1
fi
