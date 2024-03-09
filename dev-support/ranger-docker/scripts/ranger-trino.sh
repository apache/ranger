#!/bin/bash

ssh-keygen -A
/usr/sbin/sshd

if [ ! -e "${TRINO_HOME}"/.setupDone ]
then
  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" trino
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" trino
  su -c "chmod 0600 ~/.ssh/authorized_keys" trino

  cat <<EOF > /etc/ssh/ssh_config
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
EOF

  cd /opt/ranger/ranger-trino-plugin || exit
  ./enable-trino-plugin.sh

  touch "${TRINO_HOME}"/.setupDone
  echo "Ranger Trino Plugin Installation is complete!"
fi

/usr/lib/trino/bin/run-trino

TRINO_PID=$(ps -ef  | grep -v grep | grep -i "io.trino.server.TrinoServer" | awk '{print $2}')

# prevent the container from exiting
if [ -z "$TRINO_PID" ]
then
  echo "The Trino process probably exited, no process id found!"
else
  tail --pid="$TRINO_PID" -f /dev/null
fi
