<h1>Question 1</h1>
<p><h2>Pairs</h2>
There are two mapReduce jobs. For the first job, the input is Shakespeare.txt. During the mapper and reducer, I compute the  the total number of each appearance of word (X, *) and output it as (word, value). In the second job. It takes Shakespeare.txt as input. The input type is {LongWritable, Text}. During the mapper phase, compute co-occurrence of each pair of words (x,y). In the reducer phase, in the setup, it reads the result from the first job. Set the total number of each appearance of word to a hashmap. Then compute the PMI. The output will be key-value pairs where key and value are  PairOfStrings (wordA, wordB)(PMI, count)
<h2>Stripes</h2>
There are two mapReduce jobs. For the first job, the input is Shakespeare.txt. During the mapper and reducer, I compute the  the total number of each appearance of word (X, *) and output it as (word, value). In the second job. It takes Shakespeare.txt as input. The input type is {LongWritable, Text} During the mapper phase, compute co-occurrence of each pair of words {word (A,value),(B,value)...}. In the reducer phase, compute the PMI. The output will be key-value pairs where key and value are  PairOfStrings (wordA, wordB)(PMI, count)
</p>
<h1>Question 2</h1>
<p>I run it on linux.student.cs.uwaterloo.ca</p>
<p>Pairs: 27.345 seconds </p>
<p>Stripes: 12.69 seconds </p>
<h1>Question 3</h1>
<p>I run it on linux.student.cs.uwaterloo.ca</p>
<p>Pairs: 27.432</p>
<p>Stripes: 13.409</p>
<h1>Question 4</h1>
<p>77198  308792 3002062</p>
<h1>Question 5</h1>
<p>highest PMI: (maine, anjou)	(3.633142306524297, 12)
(anjou, maine)	(3.633142306524297, 12) lowest PMI: (thy, you)	(-1.5303967509748593, 11)
(you, thy)	(-1.5303967509748593, 11) which means that seeing maine has highest probability of seeing anjou.
you and thy has lowest connection</p>
<h1>Question 6</h1>
<p>('tears')
(tears, shed)	(2.111790070671056, 15)
(tears, salt)	(2.0528122046574038, 11)
(tears, eyes)	(1.1651669513642202, 23)

('death')
(death, father's)	(1.1202520352359615, 21)
(death, die)	(0.7541593868380971, 18)
(death, life)	(0.7381345933734321, 31)</p>
<h1>Question 7</h1>
<p>(hockey, defenceman)	(2.4180872078437172, 153)
(hockey, winger)	(2.370091779477945, 188)
(hockey, sledge)	(2.352184933207967, 93)
(hockey, goaltender)	(2.253738491028581, 199)
(hockey, ice)	(2.2093476349398986, 2160.0)</p>
<h1>Question 8</h1>
<p>(data, cooling)	(2.0979041149039497, 74)
(data, encryption)	(2.0443724251833877, 53)
(data, array)	(1.992630713619952, 50)
(data, storage)	(1.9878386708778675, 110)
(data, database)	(1.8893089278044872, 99)</p>
