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
        // Each processWorker() calculates its batch boundaries
        // directly from the shared batch index.
        //
        // Results are written directly into one preallocated array.
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
        const results =
            new Array(
                candidates.length
            );

        // Shared batch counter.
        let nextBatchIndex = 0;

        // ------------------------------------------------------------
        // Random worker selection.
        //
        // Preserve the original behavior:
        //
        // - Select a random worker.
        // - Do not select the same worker twice consecutively.
        // ------------------------------------------------------------

        let lastWorkerIndex = -1;

        function getRandomWorker() {
            if (workers.length === 1) {
                lastWorkerIndex = 0;
                return workers[0];
            }

            let index;

            do {
                index =
                    Math.floor(
                        Math.random() *
                        workers.length
                    );
            } while (
                index === lastWorkerIndex
            );

            lastWorkerIndex = index;

            return workers[index];
        }

        // Build this ONCE instead of allocating a new headers object
        // for every proxy request.
        //
        // This is the same header behavior as the original dispatcher:
        // forward the supplied headers and force Content-Type to
        // application/json for the dispatcher -> proxy request.
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
                    getRandomWorker();

                // ----------------------------------------------------
                // Pass the requested upstream Host to the proxy worker
                // using its existing ?host= mechanism.
                //
                // The proxy worker reads this parameter and sets the
                // Host header on the request to SmugMug.
                // ----------------------------------------------------

                const proxyUrl =
                    new URL(workerUrl);

                if (headers.Host) {
                    proxyUrl.searchParams.set(
                        "host",
                        headers.Host
                    );
                }

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
                            proxyUrl.toString(),
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
        // Run the dispatcher in waves.
        //
        // Each dispatcher worker handles ONE batch at a time.
        //
        // With:
        //
        //     concurrency = 20
        //     batchSize = 10
        //
        // we run:
        //
        //     Wave 1: 10 batches × 10 URLs = 100 URLs
        //     WAIT until all 10 batches finish
        //     Wave 2: 10 batches × 10 URLs = 100 URLs
        //
        // This prevents 20 batches from being in flight at once.
        //
        // The requested concurrency value remains unchanged; this
        // only limits the number of simultaneously active dispatcher
        // workers to 10.
        // ------------------------------------------------------------

        const WAVE_SIZE = 10;

        while (
            nextBatchIndex <
            batchCount
        ) {
            const waveCount =
                Math.min(
                    WAVE_SIZE,
                    batchCount -
                        nextBatchIndex
                );

            const tasks =
                new Array(
                    waveCount
                );

            for (
                let i = 0;
                i < waveCount;
                i += 1
            ) {
                tasks[i] =
                    processWorker();
            }

            // --------------------------------------------------------
            // IMPORTANT:
            //
            // Wait for EVERY worker in this wave to finish before
            // starting the next wave.
            // --------------------------------------------------------

            await Promise.all(
                tasks
            );
        }

        // ------------------------------------------------------------
        // Return complete JSON response.
        //
        // There is only ONE result array and ONE serialization.
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
