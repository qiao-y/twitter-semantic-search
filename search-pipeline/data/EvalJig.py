import collections

class Measure(object):
    def __init__(self):
        """The default pretty* methods will print the value of the measure using
        self.formatstr.  That's handy unless you have an unusual measure."""
        self.formatstr = '{:.4f}'

    def __str__(self): return "measure name"

    def compute(self, ranking, qrel, minrel):
        """Compute the measure for the given ranking, qrel, and minimum rel
        value."""
        return 0
        
    def redux(self, scores):
        """Compute the average. Scores is a {topic,score} dict.  The default
        is usually fine.  For an example measure with NO mean value, see
        RelString below."""
        return sum(scores.values()) / len(scores.values())

    def pretty(self, topic, value):
        """Print the measure's value."""
        return topic + " " + str(self) + " " + self.formatstr.format(value)

    def pretty_mean(self, value):
        """Print the measure's mean value."""
        return "all " + str(self) + " " + self.formatstr.format(value)

class EvalJig:
    def __init__(self):
        self.ops = []
        self.score = collections.defaultdict(dict)
        self.means = dict()
        self.topics = set()
        self.minrel = 1
        self.evaldepth = 1000
        self.verbose = False
        
    def add_op(self, meas):
        self.ops.append(meas)

    def zero(self, topic):
        self.topics.add(topic)
        for op in self.ops:
            self.score[str(op)][topic] = 0

    def compute(self, topic, ranking, qrel):
        self.topics.add(topic)
        sr = sorted(ranking, reverse=True)[:self.evaldepth]
        for op in self.ops:
            self.score[str(op)][topic] = op.compute(sr, qrel, minrel=self.minrel)

    def comp_means(self):
        for op in self.ops:
            self.means[str(op)] = op.redux(self.score[str(op)])

    def print_scores_for(self, topic):
        for op in self.ops:
            opname = str(op)
            if self.score[opname].has_key(topic):
                print op.pretty(topic, self.score[opname][topic])

    def print_scores(self):
        for topic in sorted(self.topics, key=lambda a: map(int, str(a).split('.'))):
            self.print_scores_for(topic)

    def print_means(self):
        for op in self.ops:
            opname = str(op)
            if self.means[opname] is not None:
                print op.pretty_mean(self.means[opname])

class NumRetr(Measure):
    """Number of documents retrieved."""
    def __init__(self):
        Measure.__init__(self)
        self.formatstr = '{:d}'
    def __str__(self): return 'num_ret'
    def compute(self, ranking, qrel, minrel): return len(ranking)
    def redux(self, scores): return sum(scores.values())

class NumRel(Measure):
    """Number of relevant documents."""
    def __init__(self):
        Measure.__init__(self)
        self.formatstr = '{:d}'
    def __str__(self): return 'num_rel'
    def compute(self, ranking, qrel, minrel):
        return sum(1 for rel in qrel.values() if rel >= minrel)
    def redux(self, scores): return sum(scores.values())

class RelRet(Measure):
    """Number of relevant documents retrieved."""
    def __init__(self):
        Measure.__init__(self)
        self.formatstr = '{:d}'
    def __str__(self): return 'num_rel_ret'
    def compute(self, ranking, qrel, minrel):
        return sum(1 for (s, d) in ranking if qrel.has_key(d) and qrel[d] >= minrel)
    def redux(self, scores): return sum(scores.values())

def rel_ranks(ranking, qrels, minrel=1):
    rank = 0
    for (score, doc) in ranking:
        rank += 1
        if qrels.has_key(doc) and qrels[doc] >= minrel:
            yield rank, score, doc

class AvePrec(Measure):
    """Average (uninterpolated) precision."""
    def __str__(self): return 'map'
    def compute(self, ranking, qrel, minrel=1):
        num_rel = sum(1 for rel in qrel.values() if rel >= minrel)
        if num_rel == 0:
            return 0
        rel_so_far = 0
        sum_prec = 0
        for (rank, score, doc) in rel_ranks(ranking, qrel, minrel):
            rel_so_far += 1
            sum_prec += rel_so_far / float(rank)
        return sum_prec / num_rel

class PrecAt(Measure):
    """Precision at a rank cutoff."""
    def __init__(self, cutoff):
        Measure.__init__(self)
        self.cutoff = cutoff
    def __str__(self): return 'P_{}'.format(self.cutoff)
    def compute(self, ranking, qrel, minrel=1):
        s = sum(1 for (rank, score, doc) in rel_ranks(ranking, qrel, minrel) if rank <= self.cutoff)
        return s/float(self.cutoff)

class RelString(Measure):
    """A relstring is a text visualization of a ranked list. The relevance values
    for each document are printed as a single character, with '-' meaning not
    relevant. The ranking is cut into blocks of 10 using '/' chars for
    readability."""
    def __init__(self, length=100):
        Measure.__init__(self)
        self.length = length
        self.formatstr = '{}'
    def __str__(self): return 'rel_str'
    def strgen(self, ranking, qrel):
        rank = 0
        for (score, doc) in ranking:
            rank += 1
            if rank > self.length:
                break
            if qrel.has_key(doc):
                if qrel[doc] == 0: ch = '-'
                elif 10 > qrel[doc] > 0: ch = str(qrel[doc])
                elif qrel[doc] >= 10: ch = '!'
                elif qrel[doc] < 0: ch = '.'
            else: ch = '-'
            if rank % 10 == 0:
                ch += '/'
            yield ch

    def compute(self, ranking, qrel, minrel=1):
        return ''.join(self.strgen(ranking, qrel))
    def redux(self, scores): return None

if __name__ == '__main__':
    print 'Foo'
