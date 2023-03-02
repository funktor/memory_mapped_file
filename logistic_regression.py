import numpy as np
import mmap, math, random, time, os
import pandas as pd
from multiprocessing import Lock
from concurrent.futures import ProcessPoolExecutor, wait, ThreadPoolExecutor

def loss(probs, labels, weights, bias, reg_lambda, n):
    l = 0
    for i in range(n):
        l += -labels[i]*math.log(probs[i]) if probs[i] != 0 else 0
        l += -(1-labels[i])*math.log(1-probs[i]) if probs[i] != 1 else 0
    
    l += reg_lambda*(sum([w*w for w in weights]) + bias*bias)
    
    return l
    
def gradient_descent(labels, weights, bias, num_epochs, batch_size, learning_rate, reg_lambda, n, m, feature_obj):
    probs = np.zeros(n)
    
    for epoch in range(num_epochs):
        i = 0
        while True:
            start, end = i*batch_size, min((i+1)*batch_size, n)
            data = feature_obj.get(start, end-1).decode().split(",")
            data = np.array([float(x) for x in data if x != '']).reshape(-1, m)
            
            h = bias + np.dot(weights, data.T)
            probs[start:end] = 1.0/(1.0+np.exp(-h))
            
            sub_labels = labels[start:end]
            sums = 2*reg_lambda*weights + np.dot((probs[start:end]-sub_labels), data)
            
            weights -= learning_rate*sums
            s = np.sum(probs[start:end]-sub_labels) + 2*reg_lambda*bias
                
            bias -= learning_rate*s
            
            i += 1
            
            if end == n:
                break
            
            curr_loss = loss(probs, labels, weights, bias, reg_lambda, n)
            print(epoch, curr_loss)
    
    return [weights, bias]

def init_weights(n):
    return np.random.random(n)


class Feature:
    def __init__(self, n, dim=100, file='features.txt'):
        self.lock = Lock()
        self.positions = []
        self.file = file
        self.n = n
        self.dim = dim
        self.batch_len = 1024*1024*100
        self.batch = 0
        
        if os.path.isfile(self.file):
            f = open(self.file, 'r+b')
        else:
            f = open(self.file, 'w+b')
            f.write(b'\0')
            f.flush()
        
        self.mmap_obj = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_WRITE)
        f.close()
        
    def insert(self):
        data = np.random.rand(1, self.dim)[0]
        data = ','.join([str(x) for x in data])
        data = data.encode() + b','
        
        with self.lock:
            offset = self.positions[-1] if len(self.positions) > 0 else 0
            
            if offset+len(data) >= self.batch*self.batch_len:
                f = open(self.file, 'r+b')
                f.seek(offset)
                f.write(b'\0'*self.batch_len)
                self.batch += 1
                self.mmap_obj.resize(self.batch*self.batch_len)
                f.close()
            
            # print(self.mmap_obj.size(), offset, offset+len(data))
            # print()
            self.mmap_obj[offset:offset+len(data)] = data
            self.positions += [self.positions[-1] + len(data)] if len(self.positions) > 0 else [len(data)]
    
    def get(self, start, end):
        with self.lock:
            start = min(start, len(self.positions))
            end = min(end, len(self.positions)-1)
            
            if end < start:
                return b''
            
            start_pos = self.positions[start-1] if start > 0 else 0
            end_pos = self.positions[end]
            
            return self.mmap_obj[start_pos:end_pos]
    
class StreamingLogisticRegression:
    def __init__(self, reg_lambda, epochs, batch_size, learning_rate, n, m, feature_obj):
        self.weights = []
        self.bias = 0
        self.reg_lambda = reg_lambda
        self.epochs = epochs
        self.batch_size = batch_size
        self.learning_rate = learning_rate
        self.n = n
        self.m = m
        self.feature_obj = feature_obj
    
    def train(self, labels):
        self.weights = init_weights(self.m)
        
        self.weights, self.bias = gradient_descent(labels, 
                                                   self.weights, 
                                                   self.bias, 
                                                   self.epochs, 
                                                   self.batch_size,
                                                   self.learning_rate, 
                                                   self.reg_lambda, 
                                                   self.n, 
                                                   self.m, 
                                                   self.feature_obj)

if __name__ == '__main__':
    n, m = 10000000, 1000
    
    labels = np.random.randint(0, 1, n)
    feature_obj = Feature(n, m)
    
    for i in range(n):
        feature_obj.insert()
    
    reg = StreamingLogisticRegression(0, 100, 64, 0.01, n, m, feature_obj)
    reg.train(labels)
    
    