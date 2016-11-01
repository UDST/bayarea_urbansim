## How the Orca Cache Works

We had a couple of snafus surrounding the orca cache behavior recently and it took some real digging to both fix and understand the problems.  This is a document that will record our learnings about the orca cache for future orca-ers.

### Why did setting cache=True when defining the jobs table cause the same jobs summary numbers to be printed out each year - doesn't the cache get cleared each year?

No.  As the docs clearly [state](https://udst.github.io/orca/core.html?highlight=cache_scope#cache-scope), the default cache_scope is "fovever."  You have to specify a cache_scope of "iteration" if you want that behavior.

### Really, the default is forever - shouldn't it be iteration?

Yeah probably, since the main point of Orca is to make recomputation of variables easier for each simulation iteration.

### Yeah but really, all sorts of things have a cache=True (e.g. in urbansim_defaults), don't these get recomputed every year?

Yes - it turns out that is a happy accident.  When you add rows to a table, you clear the cache, so since we add rows to jobs and households via the transition models and to buildings via the developer model, the cache gets cleared by adding those new rows.

### OK, but now explain why we have 3 zone_ids in the jobs table, one that is zone_id, one that is zone_id_x, and one that is zone_id_y?

Easy.  There is a zone_id defined on parcels, buildings, and jobs.  We need a column from parcels and a column from jobs so we merge the three tables.  The first pandas merge has two zone_ids and pd.merge appends _x and _y, then the third doesn't conflict with the first two and becomes the canonical zone_id.  Turns out an odd number of merged columns will give you what you want.

### Sort of, but one of those zone_ids was different from the others - it had nulls where the other two were defined.

This was the original problem that led us to look into this - our job summaries were incorrect.  This is caused because the ELCM runs last in order to place unplaced jobs after new buildings get built.  But when the jobs get new building ids from the ELCM, the zone_id isn't updated because it is cached.  Thus all the unplaced jobs still have a null zone_id because unplaced jobs don't have zones.  (Incidentally zone_id_x and zone_id_y were both correct because they were merged after the ELCM ran - only zone_id was incorrect cause it was stuck in the cache.)

### And the first thing you tried didn't work - just clearing the orca cache - why not?!?

At first I tried clearing the "forever" cache and this doesn't work because the Pandana global memory is stored in the forever cache.  Pandana can't be reinitialized and this was the first error I got.  When I cleared the "iteration" cache instead, which I thought would work, it did NOT because the default is "forever" not "iteration" as I had thought, so the columns I needed to clear were defined as "forever" and still in the cache.

### OK is there a way out of this madness?  Like, a simple solution?

Yeah, setting the orca default cache_scope to "iteration" instead of "forever" should do the trick.

### And why is that hard?

Because it involves all of us getting on the same page and updating our code at the same time to make sure this pretty significant change doesn't break things in the middle of an important planning process.  Given that we're all on different cycles, this is a challenge.

### And how did this ever work at all.  Presumably this worked and then stopped working (we checked into past summaries)?

Yeah, now there's an excellent question!
