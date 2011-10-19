#!/bin/bash
#
# background process to keep queue full

queue_size=100

usage() {
    echo "usage: $(basename $0) [-qsize <i>] <job-list> [extra-args]" > /dev/stderr
    echo "keeps queue near specified level (default $queue_size)" > /dev/stderr
    echo "extra args are passed to qs.sh" > /dev/stderr
    exit 1
}

while [[ "$1" =~ ^- ]]; do
    case $1 in
	-usage)
	    usage
	    ;;
	-qsize)
	    queue_size=$2
	    shift; shift
	    ;;
	*)
	    echo "error: unknown argument $1" > /dev/stderr
	    usage
	    ;;
    esac
done

# read jobs file
jobfile=$1
shift
i=0
for a in $(grep -v "^#" $jobfile); do
    job[i]=$a
    i=$(($i + 1))
done

total=$i
curr=0
echo "$total jobs (${job[0]} - ${job[$(($total - 1))]})"
while [ $curr -lt $total ]; do
    qsize=$(qstat -u $(whoami) | sed -e '1,/^-----/d' | cut -c87 | grep -v 'CE' | wc -l)
    echo "$(date) queue size: $qsize, next job: $curr (${job[$curr]})"
    while [ $qsize -lt $queue_size -a $curr -lt $total ]; do
	echo "queuing ${job[$curr]}"
	qs.sh -p ${job[$curr]} $*
#	qsub -S /bin/bash -j oe -q ser2 -l nodes=1:ppn=1,walltime=00:30:00 ${job[$curr]}
	curr=$(($curr + 1))
	qsize=$(($qsize + 1))
    done
    sleep 30
done
