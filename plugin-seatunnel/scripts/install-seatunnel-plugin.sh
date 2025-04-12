#!/bin/bash
# Determine the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Define source and destination directories
SOURCE_DIR="$SCRIPT_DIR"
SEATUNNEL_WEB_DEST_DIR="/usr/vdp/current/seatunnel-web"
PLUGIN_DIR_WEB="$SEATUNNEL_WEB_DEST_DIR/ranger-seatunnel-plugin"
PLUGIN_DIR_RANGER_ADMIN="/usr/vdp/current/ranger-admin/ews/webapp/WEB-INF/classes/ranger-plugins"
PLUGIN_DIR_RANGER_ADMIN_SEATUNNEL="$PLUGIN_DIR_RANGER_ADMIN/seatunnel"
PLUGIN_JAR="$SOURCE_DIR/lib/ranger-seatunnel-plugin-impl/ranger-seatunnel-plugin-*.jar"

copyRangerSeatunnelPlugin()
{
# Check if the destination directory exists
if [ -d "$SEATUNNEL_WEB_DEST_DIR" ]; then
  # Check if the PLUGIN_DIR_WEB exists
  if [ -d "$PLUGIN_DIR_WEB" ]; then
    # Delete the existing RANGER_SEATUNNEL_PLUGIN_DIR
    rm -rf "$PLUGIN_DIR_WEB"
  fi
  # Create the seatunnel directory
  mkdir "$PLUGIN_DIR_WEB"
  cp -r "$SOURCE_DIR/lib" "$PLUGIN_DIR_WEB"
  cp -r "$SOURCE_DIR/install" "$PLUGIN_DIR_WEB"
  cp "$SOURCE_DIR/ranger_credential_helper.py" "$PLUGIN_DIR_WEB"
  chown root:root $PLUGIN_DIR_WEB -R
  echo "ranger-seatunnel-plugin installed on Seatunnel-web."
else
  echo "Seatunnel-web is not installed on this machine."
fi
}

copyRangerAdminPlugin()
{
# Check if the ranger-plugins directory exists
if [ -d "$PLUGIN_DIR_RANGER_ADMIN" ]; then
  # Check if the seatunnel directory exists
  if [ -d "$PLUGIN_DIR_RANGER_ADMIN_SEATUNNEL" ]; then
    # Delete the existing seatunnel directory
    rm -rf "$PLUGIN_DIR_RANGER_ADMIN_SEATUNNEL"
  fi
  # Create the seatunnel directory
  mkdir -p "$PLUGIN_DIR_RANGER_ADMIN_SEATUNNEL"
  # Copy the plugin jar to the seatunnel directory
  cp $PLUGIN_JAR "$PLUGIN_DIR_RANGER_ADMIN_SEATUNNEL"
  echo "Ranger admin ranger-seatunnel-plugin installed on Ranger admin."
else
  echo "Ranger admin is not installed on this machine."
fi
}

main()
{
  copyRangerSeatunnelPlugin
  copyRangerAdminPlugin
}
main