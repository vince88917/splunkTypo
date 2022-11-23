#!/usr/bin/env python
# coding=utf-8
#
# Useful links:
#  - https://github.com/maxbachmann/Levenshtein
#  - https://pypi.org/project/fastDamerauLevenshtein/

# Input:
#  - field to compare
#  - field representing time
#  - field to partition by
#  - window time in seconds
#  - Levenshtein edit distance filter
#  - Damerau-Levenshtein edit distance filter

# Sample data generation:
"""
| makeresults
| eval data="
{\"events\":[
{\"user\":\"user1\",\"time\":\"0\",\"search\":\"blah121\",\"app\":\"app1\"},
{\"user\":\"user2\",\"time\":\"2\",\"search\":\"blah22\",\"app\":\"app24\"},
{\"user\":\"user1\",\"time\":\"3\",\"search\":\"blaxxxh11\",\"app\":\"app1\"},
{\"user\":\"user2\",\"time\":\"4\",\"search\":\"blah32\",\"app\":\"app2\"},
{\"user\":\"user3\",\"time\":\"20\",\"search\":\"blah3\",\"app\":\"appx\"}
]}"
| eval _raw=data
| spath path=events{}
| mvexpand events{}
| spath input="events{}"
| typo compfield=search partitionby=user windowtime=5 levdist=2 damdist=1 timefield=time
| table user time search app
"""

import os, sys, Levenshtein, fastDamerauLevenshtein

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from splunklib.searchcommands import dispatch, EventingCommand, Configuration, Option, validators

@Configuration()
class Typo(EventingCommand):

    compfield = Option(
        doc='''**Syntax:** **compfield=***<value>*
        **Description:** field to use as comparison.''',
        require=True)

    timefield = Option(
        doc='''**Syntax:** **timefield=***<value>*
        **Description:** field to use as event time.''',
        require=False,
        default="_time")

    partitionby = Option(
        doc='''**Syntax:** **partitionby=***<value>*
        **Description:** field to use to partition events.''',
        require=True)

    windowtime = Option(
        doc='''**Syntax:** **windowtime=***<value>*
        **Description:** maximum gap in seconds between events that should be considered.''',
        require=True)

    levdist = Option(
        doc='''**Syntax:** **levdist=***<value>*
        **Description:** maximum Levenshtein distance between events, as calculated over **compfield**. At least one of **levdist** or **damdist** should be specified. If both are, the command will return events where either clause is satisfied.''',
        require=False,
        default=0)

    damdist = Option(
        doc='''**Syntax:** **damdist=***<value>*
        **Description:** maximum Damerau-Levenshtein distance between events, as calculated over **compfield**. At least one of **levdist** or **damdist** should be specified. If both are, the logic is to return events where either clause is satisfied.''',
        require=False,
        default=0)

    def transform(self, records):
        thisLevDist=int(self.levdist)
        thisDamDist=int(self.damdist)
        recordsToYield=[]
        # Iterate over all records, partitioning into a dictionary of lists. Key for the dict will be user ID, value will be all records for that user
        recordsByUser={}
        for record in records:
            if record[self.partitionby] not in recordsByUser:
                recordsByUser[record[self.partitionby]]=[]
            recordsByUser[record[self.partitionby]].append(record)
        for partition in recordsByUser:
            # Ensure records are sorted by time
            sortedRecords = sorted(recordsByUser[partition], key=lambda c: int(c[self.timefield]))
            recordCount = len(sortedRecords)
            for record in sortedRecords:
                index = sortedRecords.index(record)
                if (index + 1) < recordCount:
                    # Is the next event within the specified window?
                    if (int(sortedRecords[index + 1][self.timefield]) - int(record[self.timefield])) <= int(self.windowtime):
                        levdist = 0
                        damdist = 0
                        if thisLevDist > 0:
                            levdist = Levenshtein.distance(record[self.compfield], sortedRecords[index + 1][self.compfield], score_cutoff = int(thisLevDist))
                        if thisDamDist > 0:
                            damdist = fastDamerauLevenshtein.damerauLevenshtein(record[self.compfield], sortedRecords[index + 1][self.compfield], similarity = False)
                        if ((levdist == thisLevDist) and (levdist > 0)) or ((damdist == thisDamDist) and (damdist > 0)):
                            recordsToYield.append(record)
                            recordsToYield.append(sortedRecords[index+1])
                    else:
                        continue
                index += 1
        for record in recordsToYield:
            yield record

dispatch(Typo, sys.argv, sys.stdin, sys.stdout, __name__)
