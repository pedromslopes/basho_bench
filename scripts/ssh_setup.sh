#!/usr/bin/env bash

ssh-keygen -t rsa

ssh-copy-id $1
ssh-copy-id $2
ssh-copy-id $3
ssh-copy-id $4
ssh-copy-id $5
