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
    #[ -d ${EXT_DIR} ] && rm -rf ${EXT_DIR}
    grep 'https://' `find . -name '*.html' -print` | grep -v '.apache.org' | grep '="https://' | awk -F'"' '{ 
        for(i = 1 ; i <= NF ; i++) {
            if ($i ~ "^https://") {
                print $i
            }
        }
    }' | sort -u > ${EXT_URL_REF_FILE}
    for url in `cat ${EXT_URL_REF_FILE}`
    do
        #echo "+ URL: [${url}] ..."
        newname=`echo $url | sed -e "s@https://@${EXT_DIR}/@"`
        #echo "$url => ./${newname}"
        dn=`dirname ${newname}`
        fn=`basename ${newname}`
        ext=`echo ${fn} | awk -F"." '{ if (NF > 1) { print $NF } }'`
        if [ -n "${ext}" ]
        then
            #echo "+ mkdir -p ${dn}"
            mkdir -p ${dn}
            #echo "+ curl -o ${newname} ${url}"
            curl -s -o ${newname} ${url}
            #echo "$url => ./${newname}"
            for f in `find . -name '*.html' -print`
            do
                grep "${url}" ${f} > /dev/null 
                if [ $? -eq 0 ]
                then
                    cat ${f} | sed -e "s|${url}|./${newname}|g" > ${f}.$$  && mv ${f}.$$ ${f}
                fi
            done
        fi
    done
    #rm -f ${EXT_URL_REF_FILE}
else
    echo "ERROR: Unable to locate the target folder - Run 'mvn site' command before kicking off this script"
    exit 1
fi
