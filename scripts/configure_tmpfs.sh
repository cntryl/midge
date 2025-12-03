#!/bin/bash
# Script to configure tmpfs size for Midge tests
#
# This script increases the size of /tmp to accommodate Midge test files.
# The default tmpfs size on many systems is 50% of RAM, but with many
# concurrent tests, we may need more space.

set -e

echo "Configuring tmpfs size for Midge tests..."

# Check current tmpfs size
CURRENT_SIZE=$(df -h /tmp | tail -1 | awk '{print $2}')
echo "Current /tmp size: $CURRENT_SIZE"

# Recommended size: 16GB for test workloads
RECOMMENDED_SIZE="16G"

echo ""
echo "To increase /tmp size temporarily (until next reboot):"
echo "  sudo mount -o remount,size=$RECOMMENDED_SIZE /tmp"
echo ""
echo "To make the change permanent, add this to /etc/fstab:"
echo "  tmpfs /tmp tmpfs defaults,size=$RECOMMENDED_SIZE 0 0"
echo ""
echo "Or create a systemd override:"
echo "  sudo mkdir -p /etc/systemd/system/tmp.mount.d"
echo "  echo -e '[Mount]\\nOptions=mode=1777,strictatime,nosuid,nodev,size=$RECOMMENDED_SIZE' | sudo tee /etc/systemd/system/tmp.mount.d/size.conf"
echo "  sudo systemctl daemon-reload"
echo "  sudo systemctl restart tmp.mount"
echo ""

read -p "Apply temporary change now? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Applying temporary tmpfs size increase..."
    sudo mount -o remount,size=$RECOMMENDED_SIZE /tmp
    echo "Done! New size:"
    df -h /tmp | tail -1
else
    echo "Skipped. You can run the commands manually when ready."
fi
