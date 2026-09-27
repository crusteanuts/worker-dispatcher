export default {
    async fetch(request, env, ctx) {
        if (request.method !== "POST") {
            return new Response(
                "Method Not Allowed",
                { status: 405 }
            );
        }

        let body;

        try {
            body = await request.json();
        } catch {
            return new Response(
                "Invalid JSON",
                { status: 400 }
            );
        }

        const workers = body.workers;
        const candidates = body.candidates;

        const method =
            String(
                body.method ?? "GET"
            ).toUpperCase();

        const headers =
            body.headers &&
                typeof body.headers === "object"
                ? body.headers
                : {};

        const concurrency =
            Number(
                body.concurrency ??
                workers?.length ??
                1
            );

        const batchSize =
            Number(
                body.batch_size ??
                10
            );

        // ------------------------------------------------------------
        // Validate request
        // ------------------------------------------------------------

        if (
            !Array.isArray(workers) ||
            workers.length === 0
        ) {
            return new Response(
                "workers must be a non-empty array",
                { status: 400 }
            );
        }

        if (!Array.isArray(candidates)) {
            return new Response(
                "candidates must be an array",
                { status: 400 }
            );
        }

        if (
            !Number.isInteger(concurrency) ||
            concurrency < 1
        ) {
            return new Response(
                "concurrency must be a positive integer",
                { status: 400 }
            );
        }

        if (
            !Number.isInteger(batchSize) ||
            batchSize < 1
        ) {
            return new Response(
                "batch_size must be a positive integer",
                { status: 400 }
            );
        }

        for (const worker of workers) {
            if (
                typeof worker !== "string" ||
                !worker
            ) {
                return new Response(
                    "Every worker must be a non-empty URL string",
                    { status: 400 }
                );
            }
        }

        for (const candidate of candidates) {
            if (
                !candidate ||
                typeof candidate.key !== "string" ||
                typeof candidate.url !== "string"
            ) {
                return new Response(
                    "Every candidate must contain string key and url",
                    { status: 400 }
                );
            }
        }

        // ------------------------------------------------------------
        // Nothing to process
        // ------------------------------------------------------------

        if (candidates.length === 0) {
            return new Response(
                "[]",
                {
                    status: 200,
                    headers: {
                        "Content-Type":
                            "application/json"
                    }
                }
            );
        }

        // ------------------------------------------------------------
        // Worker pool
        //
        // Do NOT create a batches array.
        //
        // Each dispatcher worker calculates its batch boundaries
        // directly from the batch index. This avoids creating:
        //
        //     candidates.slice(...)
        //
        // arrays for every batch.
        //
        // Results are written directly into one preallocated array.
        // This avoids:
        //
        //     one results[] per worker
        //     Promise.all() result arrays
        //     workerResults.flat()
        //
        // The proxy worker behavior remains unchanged.
        // ------------------------------------------------------------

        const batchCount =
            Math.ceil(
                candidates.length /
                batchSize
            );

        const workerCount =
            Math.min(
                concurrency,
                batchCount
            );

        // Preallocate the final result array.
        //
        // Each candidate gets exactly one result slot.
        //
        // This is important because it means we do not have to
        // accumulate separate arrays and merge them afterward.
        const results =
            new Array(
                candidates.length
            );

        // Shared batch counter.
        //
        // Each concurrent processWorker() claims exactly one batch
        // before reaching its next await.
        let nextBatchIndex = 0;

        // Round-robin worker selection.
        //
        // Each batch is assigned to the next worker in sequence.
        // This avoids the random-selection retry loop and provides
        // predictable, evenly distributed worker assignment.
        let currentWorkerIndex = 0;

        function getNextWorker() {
            const worker =
                workers[currentWorkerIndex];

            currentWorkerIndex =
                (currentWorkerIndex + 1) %
                workers.length;

            return worker;
        }

        // Build this ONCE instead of allocating a new headers object
        // for every proxy request.
        const requestHeaders = {
            ...headers,

            "Content-Type":
                "application/json"
        };

        async function processWorker() {
            while (true) {
                const batchIndex =
                    nextBatchIndex++;

                if (
                    batchIndex >=
                    batchCount
                ) {
                    return;
                }

                const start =
                    batchIndex *
                    batchSize;

                const end =
                    Math.min(
                        start + batchSize,
                        candidates.length
                    );

                const workerUrl =
                    getNextWorker();

                // ----------------------------------------------------
                // Build only the URL array needed by the proxy worker.
                //
                // This avoids creating a batch array with slice().
                // ----------------------------------------------------

                const urls =
                    new Array(
                        end - start
                    );

                for (
                    let i = start;
                    i < end;
                    i += 1
                ) {
                    urls[i - start] =
                        candidates[i].url;
                }

                try {
                    // ------------------------------------------------
                    // Send the batch to the proxy worker.
                    // ------------------------------------------------

                    const response =
                        await fetch(
                            workerUrl,
                            {
                                method: "POST",

                                headers:
                                    requestHeaders,

                                body:
                                    JSON.stringify({
                                        urls,
                                        method
                                    })
                            }
                        );

                    if (!response.ok) {
                        const errorText =
                            await response.text();

                        const errorMessage =
                            `Worker returned HTTP ${response.status}: ${errorText}`;

                        for (
                            let i = start;
                            i < end;
                            i += 1
                        ) {
                            results[i] = {
                                key:
                                    candidates[i].key,

                                url:
                                    candidates[i].url,

                                worker:
                                    workerUrl,

                                result:
                                    null,

                                error:
                                    errorMessage
                            };
                        }

                        continue;
                    }

                    let data;

                    try {
                        data =
                            await response.json();
                    } catch {
                        for (
                            let i = start;
                            i < end;
                            i += 1
                        ) {
                            results[i] = {
                                key:
                                    candidates[i].key,

                                url:
                                    candidates[i].url,

                                worker:
                                    workerUrl,

                                result:
                                    null,

                                error:
                                    "Worker returned invalid JSON"
                            };
                        }

                        continue;
                    }

                    if (
                        !Array.isArray(
                            data.results
                        )
                    ) {
                        for (
                            let i = start;
                            i < end;
                            i += 1
                        ) {
                            results[i] = {
                                key:
                                    candidates[i].key,

                                url:
                                    candidates[i].url,

                                worker:
                                    workerUrl,

                                result:
                                    null,

                                error:
                                    "Worker response missing results array"
                            };
                        }

                        continue;
                    }

                    // ------------------------------------------------
                    // Map worker results directly into the final
                    // preallocated result array.
                    //
                    // The result order remains exactly the same as
                    // the candidate order.
                    // ------------------------------------------------

                    for (
                        let offset = 0;
                        offset < end - start;
                        offset += 1
                    ) {
                        const candidateIndex =
                            start + offset;

                        const candidate =
                            candidates[
                            candidateIndex
                            ];

                        const result =
                            data.results[offset];

                        if (!result) {
                            results[
                                candidateIndex
                            ] = {
                                key:
                                    candidate.key,

                                url:
                                    candidate.url,

                                worker:
                                    workerUrl,

                                result:
                                    null,

                                error:
                                    "Missing result for candidate"
                            };

                            continue;
                        }

                        results[
                            candidateIndex
                        ] = {
                            key:
                                candidate.key,

                            url:
                                candidate.url,

                            worker:
                                workerUrl,

                            result,

                            error:
                                result.error ||
                                ""
                        };
                    }

                } catch (error) {
                    const message =
                        error instanceof Error
                            ? error.message
                            : String(error);

                    for (
                        let i = start;
                        i < end;
                        i += 1
                    ) {
                        results[i] = {
                            key:
                                candidates[i].key,

                            url:
                                candidates[i].url,

                            worker:
                                workerUrl,

                            result:
                                null,

                            error:
                                message
                        };
                    }
                }
            }
        }

        // ------------------------------------------------------------
        // Start worker pool
        // ------------------------------------------------------------

        const tasks =
            new Array(
                workerCount
            );

        for (
            let i = 0;
            i < workerCount;
            i += 1
        ) {
            tasks[i] =
                processWorker();
        }

        await Promise.all(
            tasks
        );

        // ------------------------------------------------------------
        // Return complete JSON response.
        //
        // There is now only ONE result array and ONE serialization.
        // ------------------------------------------------------------

        return new Response(
            JSON.stringify(results),
            {
                status: 200,
                headers: {
                    "Content-Type":
                        "application/json",

                    "Cache-Control":
                        "no-cache"
                }
            }
        );
    }
};
