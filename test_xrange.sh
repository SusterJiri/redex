#!/bin/bash


redis-cli XADD banana 0-1 banana mango

redis-cli XADD banana 0-2 orange raspberry

redis-cli XADD banana 0-3 grape pineapple

redis-cli XRANGE banana 0-2 0-3