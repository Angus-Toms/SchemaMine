import math
from collections import defaultdict
from hashlib import sha256
from numpy import random

class CountMinSketch:
    def __init__(self, width, depth):
        self.width = width
        self.depth = depth
        self.table = [[0] * width for _ in range(depth)]
        self.hash_seeds = [i for i in range(depth)]

    def _hash(self, item, seed):
        # Simple hash function using sha256
        return int(sha256((str(seed) + str(item)).encode()).hexdigest(), 16) % self.width

    def update(self, item):
        # Increment counts in the CMS
        for i in range(self.depth):
            self.table[i][self._hash(item, self.hash_seeds[i])] += 1

    def estimate(self, item):
        # Estimate the count using the minimum value across hash tables
        return min(self.table[i][self._hash(item, self.hash_seeds[i])] for i in range(self.depth))

# Initialize CMS and running sum
cms = CountMinSketch(width=500, depth=5)
S = 0  # Running sum

# Stream of values
COUNT = 1000
stream = random.randint(0, 1000, COUNT)

for value in stream:
    # Get the old count and old contribution
    old_count = cms.estimate(value)
    old_contribution = 0 if old_count == 0 else old_count * math.log2(old_count)
    
    # Update the CMS with the new value
    cms.update(value)
    
    # Get the new count and new contribution
    new_count = cms.estimate(value)
    new_contribution = new_count * math.log2(new_count)
    
    # Update the running sum
    S += (new_contribution - old_contribution)

    print(f"Value: {value}, New Count: {new_count}, Running Sum (S): {S}")

# Get actual entropy: sum(count * log2(count))
hashMap = {}
for value in stream:
    hashMap[value] = hashMap.get(value, 0) + 1

actual = sum(count * math.log2(count) for count in hashMap.values())
actual = math.log2(COUNT) - actual / COUNT
print(f"Approximation: {math.log2(COUNT) - S / COUNT}")
print(f"Actual Entropy: {actual}")