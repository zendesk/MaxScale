#! /bin/sh

RPMS=$(ls -1|grep '[.]rpm\|[.]deb')
echo $RPMS

for i in $RPMS
do
    mv -v $i $(echo $i|sed 's/maxscale-\(.*\)-\(.*\)[.]\(rpm\|deb\)/maxscale-\2-\1.\3/')
done
