import random

def calc_exhaustive(weights, tree=1, ch=0):
    cached = {} # (a,b) -> (value, root)
    n = len(weights)
    
    def work(a,b):
        nonlocal cached
        if a > b:
            return 0
        if (a,b) in cached:
            return cached[(a,b)][0]
                        
        summed_probs = sum(weights[i] for i in range(a,b+1))
        
        bestval = float("inf"); bestroot = None
        for j in range(a,b+1):
            # inspect j as root
            
            # value = 1 + (1 - weights[j] / summed_probs) * ( work(a, j-1) + work(j+1, b) )
            val_left  = sum(weights[i] for i in range(a, j-1+1)) / summed_probs * work(a, j-1) 
            val_right = sum(weights[i] for i in range(j+1, b+1)) / summed_probs * work(j+1, b)
            value = 1 + val_left + val_right
            if value < bestval:
                bestval = value
                bestroot = j
        
        # save in cache
        cached[(a,b)] = (bestval, bestroot)
        return bestval
        
    work(0, n-1)
    
    result = [ cached[(0,n-1)] ]
    if tree:
        t = reconstruct_tree(0, n-1, cached)
        result.append( t )
    if ch:
        result.append( cached )
    return tuple(result)
    
def calc_exhaustive(weights, tree=1, ch=0):
    """Ein Versuch Berechnungen einzusparen."""
    cached = {} # (a,b) -> (value, root)
    n = len(weights)
    
    def work(a,b):
        nonlocal cached
        if a > b:
            return 0
        if (a,b) in cached:
            return cached[(a,b)][0]
                        
        summed_probs = sum(weights[i] for i in range(a,b+1))
        
        bestval = float("inf"); bestroot = None
        for j in range(a,b+1):
            # inspect j as root
            
            # value = 1 + (1 - weights[j] / summed_probs) * ( work(a, j-1) + work(j+1, b) )
            val_left  = sum(weights[i] for i in range(a, j-1+1)) / summed_probs * work(a, j-1) 
            val_right = sum(weights[i] for i in range(j+1, b+1)) / summed_probs * work(j+1, b)
            value = 1 + val_left + val_right
            if value < bestval:
                bestval = value
                bestroot = j
        
        # save in cache
        cached[(a,b)] = (bestval, bestroot)
        return bestval
        
    work(0, n-1)
    
    result = [ cached[(0,n-1)] ]
    if tree:
        t = reconstruct_tree(0, n-1, cached)
        result.append( t )
    if ch:
        result.append( cached )
    return tuple(result)



def random_prob_array(n, resolution):
    ps = [random.randint(1, resolution) for _ in range(n)]
    s = sum(ps)
    ps = [x / s for x in ps]
    return ps
            
def reconstruct_tree(a, b, cache):
    if a > b:
        return ()
    root = cache[(a,b)][1]
    left = reconstruct_tree(a, root-1, cache)
    right = reconstruct_tree(root+1, b, cache)
    if not left and not right:
        return (root,)
    else:
        return (root, left, right)
    
def expected_runtime(n):
    return (n*n*n + 3*n*n + 2*n) / 6

def sets_to_inspect(n):
    return sum( n-s+1 for s in range(1, n+1) )
        
ps = random_prob_array(10,100)

ps1 = [0.1, 0.5, 0.4]
r, t, ch = calc_exhaustive(ps1, tree=1, ch=1)

