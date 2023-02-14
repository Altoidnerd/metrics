import matplotlib.pyplot as plt
from matplotlib import rc
import time

       
default_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

def hex_to_rgb(value):
    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))


def rgb_to_hex(rgb):
    return '#%02x%02x%02x' % rgb


def get_bar_shifts(nbars):
        x=nbars
        width = 1/x-1/2*1/x**2
        vec = []
        idx = -(x//2)
        while idx <= abs(x//2):
            vec.append(idx)
            idx+=1
        if x%2==0:
            vec.remove(0)
            return width, np.array(vec)/2
        return width, vec
    

def set_fig_size(width=18,height=10):
    fig,ax=plt.subplots(1,1)
    fig.set_size_inches(width,height)
    return fig,ax
    

def set_font_size(size=18):
    font = {'family' : 'verdana',
            'size'   : size}
    rc('font', **font)  
    
def make_big(f=18,w=18,h=10):
    set_font_size(size=f)
    fig, ax = set_fig_size(width=w,height=h)
    return fig, ax



#
# Timer magic decorater
#
def timer_func(func):
    # This function shows the execution time of 
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func

