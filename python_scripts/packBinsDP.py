

def packBinsDP(nBins, lo, hi, weights):

    d = collections.defaultdict(lambda : 0)
    lastPartSize = collections.defaultdict(lambda : 0)

    #--- Initialisierung
    
    for n in range(len(weights)):
        # Partitionierung von n Gewichten in 0 Buckets ist unmöglich
        d[(n,0)] = float("inf")
        # Partitionierung von n Gewichten in 1 Bucket geht nur 
        #   wenn alle Gewichte in dem Bucket landen
        d[(n,1)] = sum(weights[:n+1])
        # .. wobei der letzte Part dann eben jene n Gewichte umfasst
        lastPartSize[(n,1)] = n
        
    for k in range(1, nBins+1):
        # Partitionierung von 0 Gewichten in k Buckets hat Maximalgewicht 0
        d[(0,k)] = 0
        # .. wobei der letzte Part kein Gewicht umfasst
        lastPartSize[(0,k)] = 0
        
    #--- Berechnung der Teilergebnisse in der DP-Tabelle (eager)
    for k in range(2, nBins+1):
        for n in range(1, len(weights)):
            
    