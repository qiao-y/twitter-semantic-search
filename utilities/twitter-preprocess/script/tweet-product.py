#!/usr/bin/python
# Filename: compute file
import json
import string

def is_english(uchar):
		if uchar >= u'\u0000' and uchar <= u'\u007f':
			return True
		else:
			return False
			
def is_punc_norm(uchar):
	 	if (uchar >= u'\u2000' and uchar <= u'\u206f'):
			return True
		else:
			return False

def is_punc_spec(uchar):
	if (uchar >= u'\u2600' and uchar <= u'\u27bf'):
		return True
	else:
		return False

def is_punc_cjk(uchar):
	if(uchar >= u'\u3000' and uchar <= u'\u303f'):
		return True;
	else:
		return False;
		
fin = file('/Users/ranyu/Desktop/Ran/courses/2013_spring/IBM/corpus/backup/statuses.log.2013-03-31-00','r');
fout = open('tweets.2013-03-31-00.dat','w');
i = 1;
for line in fin:
	print i;
	plain = ''+line;
	data = json.loads(plain);
	if(data.has_key('text')):	
		tweet = data['text'];
		lang = data['user']['lang'];
		check = True;
		if(lang == 'en' or lang == 'id'):
			for char in tweet:
				if not isinstance(char, unicode):
					char = char.decode('utf8');
				if( (not is_english(char)) and (not is_punc_norm(char)) and (not is_punc_spec(char)) and (not is_punc_cjk(char)) ):
					check = False;
					print char;
					break;
			if(check):
				print tweet;
				fout.write(tweet.encode('utf8'));
				fout.write('\n');
	i = i+1;	
fin.close();
fout.close();
fout.close();