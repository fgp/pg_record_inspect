/*
Copyright (c) 2009, Florian G. Pflug
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer. * Redistributions in binary
form must reproduce the above copyright notice, this list of conditions and
the following disclaimer in the documentation and/or other materials provided
with the distribution. * Neither the name Florian G. Pflug nor the names
of its contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "catalog/pg_type.h"
#include "access/htup.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"

PG_MODULE_MAGIC;

Datum record_inspect_fieldinfos(PG_FUNCTION_ARGS);
Datum record_inspect_fieldvalue(PG_FUNCTION_ARGS);
Datum record_inspect_fieldvalues(PG_FUNCTION_ARGS);

/*
 * This combines get_typmodout (from lsyscache.h, but disabled there
 * with #ifndef NOT_USED) and printTypmod (from format_type.c, but
 * static there). Should this ever become part of either contrib,
 * or even postgres proper, this code duplication should be adressed
 */
static char * format_typmod(Oid typid, int32 typmod)
{
	HeapTuple	tp;
	Oid			typmodout;
	char		*res;

	/* Shouldn't be called if typmod is -1 */
	Assert(typmod >= 0);

	/* Get typmodout */

	tp = SearchSysCache(TYPEOID,
						ObjectIdGetDatum(typid),
						0, 0, 0);
	if (HeapTupleIsValid(tp))
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
		typmodout = typtup->typmodout;
		ReleaseSysCache(tp);
	}
	else
		elog(ERROR, "cache lookup failed for type %u", typid);

	/* Generate string representation */

	if (typmodout == InvalidOid)
	{
		/* Default behavior: just print the integer typmod with parens.
		   NOTE: A int32 needs at most 11 bytes in decimal notation */
		res = palloc(11 + 2 + 1);
		snprintf(res, 11 + 2 + 1, "(%d)", typmod);
	}
	else
	{
		/* Use the type-specific typmodout procedure */
		res = DatumGetCString(OidFunctionCall1(typmodout,
		                                       Int32GetDatum(typmod)));
	}

	return res;
}

PG_FUNCTION_INFO_V1(record_inspect_fieldinfos);
Datum record_inspect_fieldinfos(PG_FUNCTION_ARGS)
{
	/* Arguments */
	HeapTupleHeader	rec_header = 0;

	/* Memory context of caller */
	MemoryContext cxt_old;
	
	/* Tuple layout */
	Oid				rec_typoid;
	int32			rec_typmod;
	TupleDesc		rec_desc;
		
	/* Result cache. Everything referenced from within the cache *must*
	   be allocated in the fn_mcxt context! */
	typedef struct RecordFieldsCache {
		Oid			recinf_typoid;
		int16		recinf_typlen;
		bool		recinf_typbyval;
		char		recinf_typalign;
		TupleDesc	recinf_desc;
		
		int			max_nitems;
		NameData*	names;
		Datum*		recinfs;

		Oid			typoid;
		int32		typmod;
		ArrayType*	result;
	} RecordFieldsCache;
	RecordFieldsCache* my_cache;
	
	/* Indices */
	int				i, nitems;

	/* Extract arguments */
	if (!PG_ARGISNULL(0))
		rec_header = PG_GETARG_HEAPTUPLEHEADER(0);
	
	/* Deal gracefully with a missing STRICT declaration */
	if (!rec_header)
		PG_RETURN_NULL();

	/* Get the type's oid and typmod from the record */
	rec_typoid = HeapTupleHeaderGetTypeId(rec_header);
	rec_typmod = HeapTupleHeaderGetTypMod(rec_header);

	/* We try to be as efficient as possible if called repeatedly.
	   Hence we build the array in the fn_mcxt context and simply
	   return the previous result if the typ didn't change. If it did,
	   we still try to avoid allocations by allocating the names, oids,
	   mods and recinfs arrays in the fc_mcxt context too, and only re-allocate
	   if we need more space.
	   FIXME: Is this actually safe? Or might a caller decide to pfree
	   our result?
	*/
	my_cache = (RecordFieldsCache*) fcinfo->flinfo->fn_extra;
	if (my_cache == NULL)
	{
		/* No previous call */

		/* Create cache entry */
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
		                                              sizeof(RecordFieldsCache));
		my_cache = (RecordFieldsCache*) fcinfo->flinfo->fn_extra;

		/* Fetch storage information for then record_info composite type.
		   We assume the function was declared with a return type of record_info[]! */

		my_cache->recinf_typoid = get_element_type(get_fn_expr_rettype(fcinfo->flinfo));
		if (my_cache->recinf_typoid == InvalidOid)
			elog(ERROR, "record_inspect_fieldinfos' return type is not an array type");
		
		get_typlenbyvalalign(my_cache->recinf_typoid,
		                     &my_cache->recinf_typlen,
		                     &my_cache->recinf_typbyval,
		                     &my_cache->recinf_typalign);
		
		cxt_old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		my_cache->recinf_desc = lookup_rowtype_tupdesc_copy(my_cache->recinf_typoid, -1);
		MemoryContextSwitchTo(cxt_old);
		if (!my_cache->recinf_desc)
			elog(ERROR, "record_inspect_fieldinfos' return type is not an array-of-composite type");
		
		/* Initialize */

		my_cache->max_nitems = 0;
		my_cache->names = 0;
		my_cache->recinfs = 0;

		my_cache->typoid = InvalidOid;
		my_cache->typmod = -1;
		my_cache->result = 0;
	}
	
	if (my_cache->typoid == rec_typoid &&
	    my_cache->typmod == rec_typmod)
	{
		/* Cached result still valid. */
		PG_RETURN_ARRAYTYPE_P(my_cache->result);
	}
	
	/* Get the record's description */
	rec_desc = lookup_rowtype_tupdesc(rec_typoid, rec_typmod);
	
	/* Make sure the names and recinfs arrays are appropriately sized */
	if (my_cache->max_nitems < rec_desc->natts ||
	    !my_cache->names ||
	    !my_cache->recinfs)
	{
		#define RecordFieldsCacheInitArray(array, type, size) \
			if (array) { \
				pfree(array); \
				array = 0; \
			} \
			array = (type*) MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, \
			                                   size * sizeof(type));
		
		RecordFieldsCacheInitArray(my_cache->names  , NameData, rec_desc->natts);
		RecordFieldsCacheInitArray(my_cache->recinfs, Datum   , rec_desc->natts);
		
		#undef RecordFieldsCacheInitArray
		
		my_cache->max_nitems = rec_desc->natts;
	}
	
	/* We'll cache the result, hence allocate everything in the fn_mcxt */
	cxt_old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);	
		
	/* Convert attribute meta data to record_info composite type */
	nitems = 0;
	for (i = 0; i < rec_desc->natts; i++)
	{
		Datum 		recinf_values[3];
		bool		recinf_nulls[3] = {0, 0, 0};
		HeapTuple	recinf;
		
		if (rec_desc->attrs[i]->attisdropped)
			continue;
		
		memcpy(&(my_cache->names[nitems]),
		       &(rec_desc->attrs[i]->attname),
		       NAMEDATALEN);
		
		/* Get Datum representations of name and typoid */

		recinf_values[0] = NameGetDatum(&(my_cache->names[nitems]));
		recinf_nulls[0] = false;

		recinf_values[1] = ObjectIdGetDatum(rec_desc->attrs[i]->atttypid);
		recinf_nulls[1] = false;

		/* Get external typmod representation */
		if (rec_desc->attrs[i]->atttypmod >= 0)
		{
			recinf_values[2] = PointerGetDatum(cstring_to_text(format_typmod(rec_desc->attrs[i]->atttypid,
			                                                                 rec_desc->attrs[i]->atttypmod)));
			recinf_nulls[2] = false;
		}
		else
			recinf_nulls[2] = true;
		
		/* Build tuple and store into recinfs.
		   FIXME: Is this safe? record_in copies the data out of tuple.t_data
		   'cause they're allocated as part of a larger chunk, but
		   might be pfree'd individually. 
		 */
		recinf = heap_form_tuple(my_cache->recinf_desc, recinf_values, recinf_nulls);
		my_cache->recinfs[nitems] = HeapTupleGetDatum(recinf);
		
		++nitems;
	}
	
	/* Done witht the tuple descriptor */
	ReleaseTupleDesc(rec_desc);
	
	/* Generate result array and update cache. */
	my_cache->result = construct_array(my_cache->recinfs, nitems,
		                               my_cache->recinf_typoid, my_cache->recinf_typlen,
		                               my_cache->recinf_typbyval, my_cache->recinf_typalign);
	my_cache->typoid = rec_typoid;
	my_cache->typmod = rec_typmod;
	
	/* Restore previous context */
	MemoryContextSwitchTo(cxt_old);

	/* FIXME: Is this safe? record_in at least expects it's result to be free'd, so
	   maybe we should call construct_array on each invokation? But then, whats the
	   point of freeing an array but not it's elements?
	 */
	PG_RETURN_ARRAYTYPE_P(my_cache->result);
}

typedef struct CastPlan {
	/* The pair of types output_exprstate is valid for */
	Oid input_typoid;
	int32 input_typmod;
	
	Oid output_typoid;
	int32 output_typmod;
	
	/* Executor state for execute output_exprstate */
	EState* estate;
	
	/* The "plan" used to execute the cast */
	ExprState* output_exprstate;
	
} CastPlan;

#define isCastPlanValid(cplan) (cplan->output_exprstate != NULL)

static
CastPlan*
makeCastPlan(Oid input_typoid, int32 input_typmod,
             Oid output_typoid, int32 output_typmod)
{
	CastPlan* cplan;
	Node* expr;
	
	/* Setup the cast state struct */
	cplan = (CastPlan*) palloc0(sizeof(CastPlan));
	cplan->input_typoid = input_typoid;
	cplan->input_typmod = input_typmod;
	cplan->output_typoid = output_typoid;
	cplan->output_typmod = output_typmod;
	cplan->estate = NULL;
	cplan->output_exprstate = NULL;
	
	/* Generate an expression tree to cast from the input to the output type.
	   We'll need to inject the input value into the prepared plan. We could
	   us the parameter machinery for that, but that seems like overkill. Instead,
	   we use the CoerceToDomainValue mechanism, which seems to be perfect for
	   our purpose.
	 */
	{
		CoerceToDomainValue* coercetodomainvalue_expr = makeNode(CoerceToDomainValue);
		coercetodomainvalue_expr->typeId = input_typoid;
		coercetodomainvalue_expr->typeMod = input_typmod;
		coercetodomainvalue_expr->location = -1;
		
		expr = (Node*) coercetodomainvalue_expr;
	}
	if (exprType(expr) == output_typoid &&
	    exprTypmod(expr) == output_typmod)
	{
		goto expr_done;
	}
	
	/* Handle UNKNOWN input types by creating a CoerceViaIO node. This is necessary,
	   because coerce_to_target_type does not deal with UNKNOWN input types nicely (
	   See below). In the parser, this case seemingly does not arise, since coercions
	   of UNKNOWN typed values (literals) are done at prepare time, not at run time.
	*/
	if (exprType(expr) == UNKNOWNOID)
	{
		CoerceViaIO *iocoerce_expr = makeNode(CoerceViaIO);
		iocoerce_expr->arg = (Expr*) expr;
		iocoerce_expr->resulttype = output_typoid;
		iocoerce_expr->coerceformat = COERCE_EXPLICIT_CAST;
		iocoerce_expr->location = -1;
		
		expr = (Node*) iocoerce_expr;
	}
	if (exprType(expr) == output_typoid &&
	    exprTypmod(expr) == output_typmod)
	{
		goto expr_done;
	}
	
	/* This builds an expression tree containing everything necessary for the cast.
	   Note: coerce_to_target_type promises to return NULL if the coercion is
	         not possible. The promise, however, does *NOT* hold in all cases!.
	         It depends on can_coerce_type returning FALSE if coerce_type would throw,
	         which is e.g. false for input type UNKNOWN. We handle that case above,
	         but there might be other. It's relatively harmless (since it only results
	         in a less descriptive error message), but still something to watch out for.
	 */
	expr = (Node*) coerce_to_target_type(NULL,
	                                     expr, cplan->input_typoid,
	                                     cplan->output_typoid, cplan->output_typmod,
	                                     COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
	                                     -1 /* location */);
	if (exprType(expr) == output_typoid &&
	    exprTypmod(expr) == output_typmod)
	{
		goto expr_done;
	}
	
	/* Couldn't find an coercion expression. (Otherwise, would have jumped
	   to expr_done). Return a cast plan for which isCastPlanValid returns false.
	*/
	return cplan;
	
expr_done:
	/* Create an executor state and prepare the cast expression for execution.
	   NOTE: There is no matching FreeExecutorState() call - we trust that it'll
	   go away when our memory context is destroyed
	*/
	cplan->estate = CreateExecutorState();
	cplan->output_exprstate = ExecPrepareExpr((Expr*) expr, cplan->estate);
	
	return cplan;
}

static
Datum
execCastPlan(CastPlan* cplan, Datum input, bool input_null, bool* output_null)
{
	ExprContext* econtext;
	Datum result;
	
	/* Create the expression context used to evaluate the cast expression */
	econtext = CreateExprContext(cplan->estate);
	
	/* Inject the input datum into the plan and execute it.
	   Note: We do *not* use any special memory context for execution,
	   because a) we want the resulting datum to be allocated in our caller's
	              memory context, and
	           b) we hope that our simple cast expressions don't produce *that*
	              much junk during execution (The junk *will* eventuall be cleaned
	              up, but only when our caller's memory context is resetted.)
	*/
	econtext->domainValue_datum = input;
	econtext->domainValue_isNull = input_null;
	*output_null = true;
	result = ExecEvalExpr(cplan->output_exprstate, econtext,
	                      output_null, NULL);
	
	/* Destroy the context again */
	FreeExprContext(econtext, true);
	
	return result;
}

static
bool
coerceDatum(Oid input_typoid, int32 input_typmod,
            Datum input, bool input_null,
            Oid output_typoid, int32 output_typmod,
            Datum* output, bool* output_null,
            List** cplans, MemoryContext cplans_mcxt)
{
	CastPlan* cplan;
	ListCell* c;
	
	/* Coerce the datum value to the output type if necessary */
	if (input_typoid != output_typoid ||
	    input_typmod != output_typmod)
	{
		/* Input and output typeoid or typmod differ */
		
		/* Search existing cast plans for a matchine one */
		cplan = NULL;
		foreach(c, *cplans)
		{
			cplan = lfirst(c);

			if (cplan->input_typoid == input_typoid &&
			    cplan->input_typmod == input_typmod &&
			    cplan->output_typoid == output_typoid &&
			    cplan->output_typmod == output_typmod)
			{
				/* Found matching cast plan */
				break;
			}
			else
			{
				/* Reset so that we can distinguish between running off the end
				   of the list and breaking because we found a plan */
				cplan = NULL;
			}
		}

		/* Create a new cast plan if no previous plan matches (and add it to the list)*/
		if (cplan == NULL)
		{
			/* Create the cast plan in the fn_mcxt memory context since we want
			   to re-use it for subsequent calls. Also store it in the plan list,
			   since otherwise we'd have to switch contexts a second time.
			 */
			
			MemoryContext cxt_old = MemoryContextSwitchTo(cplans_mcxt);
			
			cplan = makeCastPlan(input_typoid, input_typmod,
			                     output_typoid, output_typmod);
			*cplans = lcons(cplan, *cplans);
			
			MemoryContextSwitchTo(cxt_old);
		}
		
		if (isCastPlanValid(cplan))
		{
			/* Execute the plan */
			*output = execCastPlan(cplan, input, input_null, output_null);
			return true;
		}
		else
			/* Report failure to caller */
			return false;
	}
	else
	{
		/* Input and output typeoid and typmod are the same */
		*output_null = input_null;
		*output = input;
		return true;
	}
}

PG_FUNCTION_INFO_V1(record_inspect_fieldvalue);
Datum record_inspect_fieldvalue(PG_FUNCTION_ARGS)
{
	/* Arguments & Result */
	
	/* Record (arg 0) */
	bool			rec_null = PG_ARGISNULL(0);
	HeapTupleHeader	rec_header = rec_null ? 0 : PG_GETARG_HEAPTUPLEHEADER(0);
	HeapTupleData	rec_data;
	
	/* Field (arg 1) */
	bool			fieldname_null = PG_ARGISNULL(1);
	NameData*		fieldname = fieldname_null ? 0 : PG_GETARG_NAME(1);
	AttrNumber		field_attno;
	Oid				field_typoid;
	int32			field_typmod;
	Datum			fieldval;
	bool			fieldval_null;
	
	/* Default value (arg 2) */
	bool			defval_null = PG_ARGISNULL(2);
	Datum			defval = defval_null ? 0 : PG_GETARG_DATUM(2);
	
	/* Coerce value if the types are not the same? */
	bool			coerce = PG_ARGISNULL(3) ? true : PG_GETARG_BOOL(3);
	
	/* Return type & value */
	Oid				output_typoid = get_fn_expr_argtype(fcinfo->flinfo, 2);
	int32			output_typmod = -1;
	Datum			outval = 0;
	bool			outval_null = true;

	/* Tuple layout */
	Oid				rec_typoid;
	int32			rec_typmod;
		
	/* Result cache. Everything referenced from within the cache *must*
	   be allocated in the fn_mcxt context! */
	typedef struct RecordValueCache {
		Oid			rec_typoid;
		int32		rec_typmod;
		TupleDesc	rec_typdesc;
		List*		cplans;
	} RecordValueCache;
	RecordValueCache* my_cache;
	
	/* Indices */
	int				i;
	
	/* Return null if the record is null */
	if (rec_null)
			PG_RETURN_NULL();
	
	/* Return null if the field is null */
	if (fieldname_null)
		PG_RETURN_NULL();

	/* Get the type's oid and typmod from the record */
	rec_typoid = HeapTupleHeaderGetTypeId(rec_header);
	rec_typmod = HeapTupleHeaderGetTypMod(rec_header);
		
	/* Fetch or create & initialize cache */
	my_cache = (RecordValueCache*) fcinfo->flinfo->fn_extra;
	if (my_cache == NULL)
	{
		/* No previous call */

		/* Create cache entry */
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
		                                              sizeof(RecordValueCache));
		my_cache = (RecordValueCache*) fcinfo->flinfo->fn_extra;
		
		/* Initialize */
		
		my_cache->rec_typoid = InvalidOid;
		my_cache->rec_typmod = -1;
		my_cache->rec_typdesc = 0;
		my_cache->cplans = NIL;
	}
	
	/* Fetch tuple description if the typoid or typmod has changed */
	if (my_cache->rec_typoid != rec_typoid ||
	    my_cache->rec_typmod != rec_typmod)
	{
		/* Note: We leak the old tuple description. One'd need to call this
		         function with the same fn_mcxt but a large number of different
		         record types for this to matter, which seems like an extreme
		         corner case.
		 */
		MemoryContext cxt_old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);		
		my_cache->rec_typdesc = lookup_rowtype_tupdesc_copy(rec_typoid, rec_typmod);		
		MemoryContextSwitchTo(cxt_old);
		
		my_cache->rec_typoid = rec_typoid;
		my_cache->rec_typmod = rec_typmod;
	}
	
	/* Find the requested field */
	field_attno = InvalidAttrNumber;
	for(i = 0; i < my_cache->rec_typdesc->natts; ++i)
	{
		Form_pg_attribute att = my_cache->rec_typdesc->attrs[i];

		if (memcmp(&(att->attname), fieldname, NAMEDATALEN) != 0)
			continue;
		
		field_attno = att->attnum;
		field_typoid = att->atttypid;
		field_typmod = att->atttypmod;
		break;
	}
	
	/* Complain if field does not exist found */
	if (field_attno == InvalidAttrNumber)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_UNDEFINED_COLUMN),
		         errmsg("field \"%s\" does not exist in record type <%s>",
		                fieldname->data,
		                format_type_be(rec_typoid))));
	}
	
	/* Complain if the field type is unset (Just for safety) */
	if (field_typoid == InvalidOid)
	{
		elog(ERROR, "type of field \"%s\" not set for record type <%s>",
		            fieldname->data,
		            format_type_be(rec_typoid));
	}
	
	/* Complain if the field type does not match the output type and
	   the user told us not to coerce.
	   NOTE: We currently generate a cast plan even for matching types,
	         since the typemods might not be the same. At the moment
	         thats a bit pointless since the output typemod is always
	         -1, but once I figure out how to query it, it'll make sense.
	 */
	if (!coerce && (field_typoid != output_typoid))
	{
		ereport(ERROR,
		        (errcode(ERRCODE_DATATYPE_MISMATCH),
		         errmsg("field \"%s\" of record type <%s> has type <%s> but output type is <%s> and coercion is disabled",
		                fieldname->data,
		                format_type_be(rec_typoid),
		                format_type_be(field_typoid),
		                format_type_be(output_typoid))));
	}
	
	/* Fetch field datum */
	rec_data.t_len = HeapTupleHeaderGetDatumLength(rec_header);
	ItemPointerSetInvalid(&(rec_data.t_self));
	rec_data.t_tableOid = InvalidOid;
	rec_data.t_data = rec_header;
	fieldval = heap_getattr(&rec_data,
	                        field_attno,
	                        my_cache->rec_typdesc,
	                        &fieldval_null);
	
	/* Coerce the field value to the output type. The list
	   my_cache->cplans is used to store the created cast plans,
	   which'll be reused on later calls.
	*/
	if (!coerceDatum(field_typoid, field_typmod,
	                 fieldval, fieldval_null,
	                 output_typoid, output_typmod,
	                 &outval, &outval_null,
	                 &my_cache->cplans, fcinfo->flinfo->fn_mcxt))
	{
		ereport(ERROR,
		        (errcode(ERRCODE_CANNOT_COERCE),
		         errmsg("field \"%s\" of record type <%s> has type <%s> which cannot be coerced to <%s>",
		                fieldname->data,
		                format_type_be(rec_typoid),
		                format_type_be(field_typoid),
		                format_type_be(output_typoid))));		
	}
	
	/* If we'd return NULL, return the default value instead */
	if (outval_null)
	{
		outval_null = defval_null;
		outval = defval;
	}
	
	/* And finally return the value */
	if (outval_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(outval);
}

PG_FUNCTION_INFO_V1(record_inspect_fieldvalues);
Datum record_inspect_fieldvalues(PG_FUNCTION_ARGS)
{
	/* Arguments & Result */
	
	/* Record (arg 0) */
	bool			rec_null = PG_ARGISNULL(0);
	HeapTupleHeader	rec_header = rec_null ? 0 : PG_GETARG_HEAPTUPLEHEADER(0);
	HeapTupleData	rec_data;
	
	/* Default value (arg 1) */
	bool			defval_null = PG_ARGISNULL(1);
	Datum			defval = defval_null ? 0 : PG_GETARG_DATUM(1);
	
	/* Coerce value if the types are not the same? */
	bool			coerce = PG_ARGISNULL(2) ? true : PG_GETARG_BOOL(2);
	
	/* Return type */
	Oid				output_typoid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	int32			output_typmod = -1;

	/* Tuple layout */
	Oid				rec_typoid;
	int32			rec_typmod;
		
	/* Result cache. Everything referenced from within the cache *must*
	   be allocated in the fn_mcxt context! */
	typedef struct RecordValueCache {
		Oid			out_typoid;
		int16		out_typlen;
		bool		out_typbyval;
		char		out_typalign;
		Oid			rec_typoid;
		int32		rec_typmod;
		TupleDesc	rec_typdesc;
		List*		cplans;
	} RecordValueCache;
	RecordValueCache* my_cache;
	
	/* Array contents */
	int				ary_lbs[1] = {1};
	Datum*			ary_datums;
	bool*			ary_nulls;
	int				ary_nitems;
	ArrayType*		ary;
	
	/* Indices */
	int				i;
	
	/* Return default value if record is null */
	if (rec_null)
		PG_RETURN_NULL();
	
	/* Get the type's oid and typmod from the record */
	rec_typoid = HeapTupleHeaderGetTypeId(rec_header);
	rec_typmod = HeapTupleHeaderGetTypMod(rec_header);
		
	/* Fetch or create & initialize cache */
	my_cache = (RecordValueCache*) fcinfo->flinfo->fn_extra;
	if (my_cache == NULL)
	{
		/* No previous call */

		/* Create cache entry */
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
		                                              sizeof(RecordValueCache));
		my_cache = (RecordValueCache*) fcinfo->flinfo->fn_extra;
		
		/* Initialize */
		
		my_cache->out_typoid = InvalidOid;
		my_cache->out_typlen = 0;
		my_cache->out_typbyval = false;
		my_cache->out_typalign = '\0';
		my_cache->rec_typoid = InvalidOid;
		my_cache->rec_typmod = -1;
		my_cache->rec_typdesc = 0;
		my_cache->cplans = NIL;
	}
	
	/* Fetch output type info if the output type has changed.
	   NOTE: This is pretty unlikely, but since we need the cache
	         anyway, we might as well be on the safe side
	 */
	if (my_cache->out_typoid != output_typoid)
	{
		get_typlenbyvalalign(output_typoid,
		                     &my_cache->out_typlen,
		                     &my_cache->out_typbyval,
		                     &my_cache->out_typalign);
		my_cache->out_typoid = output_typoid;
	}
	
	/* Fetch tuple description if the typoid or typmod has changed */
	if (my_cache->rec_typoid != rec_typoid ||
	    my_cache->rec_typmod != rec_typmod)
	{
		/* Note: We leak the old tuple description. One'd need to call this
		         function with the same fn_mcxt but a large number of different
		         record types for this to matter, which seems like an extreme
		         corner case.
		 */
		MemoryContext cxt_old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);		
		my_cache->rec_typdesc = lookup_rowtype_tupdesc_copy(rec_typoid, rec_typmod);		
		MemoryContextSwitchTo(cxt_old);
		
		my_cache->rec_typoid = rec_typoid;
		my_cache->rec_typmod = rec_typmod;
	}
	
	/* Prepare heap tuple structure */
	rec_data.t_len = HeapTupleHeaderGetDatumLength(rec_header);
	ItemPointerSetInvalid(&(rec_data.t_self));
	rec_data.t_tableOid = InvalidOid;
	rec_data.t_data = rec_header;	
	
	/* Collect datums */
	ary_datums = palloc(my_cache->rec_typdesc->natts * sizeof(Datum));
	ary_nulls  = palloc(my_cache->rec_typdesc->natts * sizeof(bool));
	ary_nitems = 0;
	for(i = 0; i < my_cache->rec_typdesc->natts; ++i)
	{
		Form_pg_attribute att = my_cache->rec_typdesc->attrs[i];
		bool att_value_null;
		Datum att_value = heap_getattr(&rec_data,
		                               att->attnum,
		                               my_cache->rec_typdesc,
		                               &att_value_null);
		
		/* If the attribute and output types differ, either coerce
		   the value (and complain if not possible) if the user requested
		   coercion, or simply skip the value
		 */
		if (att->atttypid != output_typoid ||
		    att->atttypmod != output_typmod)
		{
			if (coerce)
			{
				/* Different types, coercion requested => Coerce or report error */
				if (!coerceDatum(att->atttypid, att->atttypmod,
				                 att_value, att_value_null,
				                 output_typoid, output_typmod,
				                 &att_value, &att_value_null,
				                 &my_cache->cplans, fcinfo->flinfo->fn_mcxt))
				{
					ereport(ERROR,
					        (errcode(ERRCODE_CANNOT_COERCE),
					         errmsg("field \"%s\" of record type <%s> has type <%s> which cannot be coerced to <%s> but coercion was requested",
					                att->attname.data,
					                format_type_be(rec_typoid),
					                format_type_be(att->atttypid),
					                format_type_be(output_typoid))));
				}
			}
			else
			{
				/* Different types, no coercion requested => Skip the attribute */
				continue;
			}
		}
		
		/* Replace nulls by the default value */
		if (!att_value_null)
		{
			ary_nulls[ary_nitems] = att_value_null;
			ary_datums[ary_nitems] = att_value;
		}
		else
		{
			ary_nulls[ary_nitems] = defval_null;
			ary_datums[ary_nitems] = defval;
		}
		++ary_nitems;
	}
	
	ary = construct_md_array(ary_datums, ary_nulls,
                             1, &ary_nitems, ary_lbs,
                             output_typoid,
                             my_cache->out_typlen,
                             my_cache->out_typbyval,
                             my_cache->out_typalign);
	PG_RETURN_ARRAYTYPE_P(ary);
}
