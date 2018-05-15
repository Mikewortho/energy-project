#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 16 20:26:11 2018

@author: robertjohnson
"""

from pyspark.sql import SQLContext       
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('appName').setMaster('local[*]').set("spark.executor.memory", "2g")
sc = SparkContext.getOrCreate(conf=conf)
spark = SQLContext(sc)
sc.stop()