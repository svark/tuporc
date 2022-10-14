TODO

+ [] OPT
+ []  INIT
+ []  SQLITE3
+ [] ADDITIONS
+ [] TRACK DELETIONS
+ [] MODIFICATIONS
+ [] RULE EXEC

Groups should have matches operator when they are consumed and foreach should be supported
<grp>[*.tsf] 

Tup tries to delete outputs before building. This should be avoided to improve linking performance.
Many command read the output before updating the output. This may also mean only some outputs are written to
among the ones specified. For example, many times MSVC may not update .lib when linking a dll