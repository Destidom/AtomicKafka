#!/bin/bash

# for n in {01..04}; do
#     for T in {005,025,050,060,070,080,090,100,110,120,130} ; do
#         ansible-playbook -v -f 10 -i hosts latencyvsthroughput.yml -e "backend=gorums throughput=$T output=${n} maxentries=1024 clients=200 time=60s order=true";
#     done;
# done;

for n in {01..02}; do
    for T in {090,100}; do
    # for T in {150,180,210}; do
        ansible-playbook -v -f 10 -i hosts latencyvsthroughput.yml -e "backend=gorums throughput=$T output=${n} maxentries=64 maxaebuffer=0 clients=200 time=120s order=true syncronous=false quiet=true";
    done;
done;

# for n in {01..04}; do
#     for T in {005,025,050,060,070,080,090,100,110,120,130} ; do
#         ansible-playbook -v -f 10 -i hosts latencyvsthroughput.yml -e "backend=gorums throughput=$T output=${n} maxentries=1024 clients=200 time=60s";
#     done;
# done;

for n in {01..02}; do
    # for T in {150,180,210}; do
    for T in {080,090,100}; do
        ansible-playbook -v -f 10 -i hosts latencyvsthroughput.yml -e "backend=gorums throughput=$T output=${n} maxentries=64 maxaebuffer=0 clients=200 time=120s  order=false syncronous=false quiet=true";
    done;
done;

for n in {01..02}; do
    # for T in {150,180,210}; do
    for T in {090,100}; do
        ansible-playbook -v -f 10 -i hosts latencyvsthroughput.yml -e "backend=gorums throughput=$T output=${n} maxentries=64 maxaebuffer=10 clients=200 time=120s  order=false syncronous=false quiet=true";
    done;
done;

# for n in {01..04}; do
#     for T in {005,025,050,060,070,080,090,100,110,120} ; do
#         ansible-playbook -v -f 10 -i hosts latencyvsthroughput.yml -e "backend=hashicorp throughput=$T output=${n} maxentries=1024 clients=200 time=60s";
#     done;
# done;

# for n in {01..04}; do
#     for T in {005,010,020,030,040,050,060,070,080}; do
#         ansible-playbook -v -f 10 -i hosts latencyvsthroughput.yml -e "backend=hashicorp throughput=$T output=${n} maxentries=64 clients=200 time=60s";
#     done;
# done;

# for n in {01..04}; do
#     for T in {005,025,050,060,070,080,090,100,110,120} ; do
#         ansible-playbook -v -f 10 -i hosts latencyvsthroughput.yml -e "backend=etcd throughput=$T output=${n} maxentries=1024 clients=200 time=60s";
#     done;
# done;

# for n in {01..04}; do
#     for T in {005,010,020,030,040,050,060,070,080}; do
#         ansible-playbook -v -f 10 -i hosts latencyvsthroughput.yml -e "backend=etcd throughput=$T output=${n} maxentries=64 clients=200 time=60s";
#     done;
# done;
