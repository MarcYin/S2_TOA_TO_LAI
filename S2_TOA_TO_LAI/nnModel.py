import numpy as np
from numba import jit
@jit(nopython=True)
def affine_forward(x, w, b):
    """
    Forward pass of an affine layer
    :param x: input of dimension (D, )
    :param w: weights matrix of dimension (D, M)
    :param b: biais vector of dimension (M, )
    :return output of dimension (M, ), and cache needed for backprop
    """
    out = np.dot(x, w) + b
    cache = (x, w)
    return out, cache

@jit(nopython=True)
def relu_forward(x):
    """ Forward ReLU
    """
    out = np.maximum(np.zeros(x.shape).astype(np.float32), x)
    cache = x
    return out, cache

def predict(inputs, arrModel):
    nLayers = int(len(arrModel) / 2)
    r = inputs
    for i in range(nLayers):
        w, b = arrModel[i*2], arrModel[i*2 + 1]
        a, _ = affine_forward(r, w, b) 
        r, _ = relu_forward(a)
    return r
