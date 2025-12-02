#!/bin/bash
# Clean up leaked Midge test directories
#
# This script removes temporary directories left behind by interrupted
# or panicked tests. Safe to run at any time - only removes directories
# matching Midge test patterns.

set -e

echo "Cleaning up Midge test directories in /tmp..."

# Count directories before cleanup
BEFORE=$(find /tmp -maxdepth 1 -type d \( -name 'midge_test_*' -o -name 'midge-mem' \) 2>/dev/null | wc -l)

if [ "$BEFORE" -eq 0 ]; then
    echo "No leaked test directories found."
    exit 0
fi

echo "Found $BEFORE test directories to clean up."

# Show disk space before
echo ""
echo "Disk space before cleanup:"
df -h /tmp | tail -1

# Remove directories
find /tmp -maxdepth 1 -type d \( -name 'midge_test_*' -o -name 'midge-mem' \) -exec rm -rf {} + 2>/dev/null || true

# Count after
AFTER=$(find /tmp -maxdepth 1 -type d \( -name 'midge_test_*' -o -name 'midge-mem' \) 2>/dev/null | wc -l)
CLEANED=$((BEFORE - AFTER))

echo ""
echo "Cleaned up $CLEANED directories."
echo ""
echo "Disk space after cleanup:"
df -h /tmp | tail -1
