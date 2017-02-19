
pyspark
-------

This is the public facing repo.  It will not contain private IP data.

contains various code snipplet for spark using python
trial code as taxonomy_reporter is revamped to use hadoop spark and SparkSQL

Plan is to code generic, non-IP code here with personal hobby time.
Then if anything is usable later on, it maybe "forked" or migrated to private repo (eg in bitbucket).

eg:: 

	node2trace.py, trace_load.py
	tba --> taxoTraceTbl.py

----

rst quirkyness
--------------

i prefer .rst over .md, but there are still some issue.
notably, simple hard line break is not heeded.
thus, if have several lines that should read more like block, need to mark it as preformatted,  which is a paragraph ending with WORD::  and then an empty line, and subsequent line as "block quote" as to be indented.  the preformat ends when indent block ends.  see config:: below.  Qucik Ref: http://docutils.sourceforge.net/docs/user/rst/quickstart.html#preformatting-code-samples




----

config::

	git init

	git config --global user.email "tin6150@gmail.com" 
	git config --global user.name tin
	git config --global credential.helper 'cache --timeout=3600'
	git config --global github.user   tin6150

	git add *
	git commit -m "first commit"
	git remote add origin https://github.com/tin6150/pyspark.git
	git push -u origin master


rst eg... end of preformat here :)

