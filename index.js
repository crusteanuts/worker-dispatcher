export default {
    async fetch(request, env, ctx) {
        if (request.method !== "POST") {
            return new Response("Method Not Allowed", { status: 405 });
        }

        let body;

        try {
            body = await request.json();
        } catch {
            return new Response("Invalid JSON", { status: 400 });
        }

        const workers = body.workers;
        const candidates = body.candidates;
        const method = String(body.method ?? "GET").toUpperCase();
        const headers = body.headers ?? {};
        const concurrency = Number(
            body.concurrency ?? workers?.length ?? 1
        );
        const batchSize = Number(body.batch_size ?? 10);

        // ------------------------------------------------------------
        // Validate request
        // ------------------------------------------------------------

        if (!Array.isArray(workers) || workers.length === 0) {
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

        if (!Number.isInteger(concurrency) || concurrency < 1) {
            return new Response(
                "concurrency must be a positive integer",
                { status: 400 }
            );
        }

        if (!Number.isInteger(batchSize) || batchSize < 1) {
            return new Response(
                "batch_size must be a positive integer",
                { status: 400 }
            );
        }

        if (typeof method !== "string" || !method) {
            return new Response(
                "method must be a non-empty string",
                { status: 400 }
            );
        }

        if (
            headers === null ||
            typeof headers !== "object" ||
            Array.isArray(headers)
        ) {
            return new Response(
                "headers must be an object",
                { status: 400 }
            );
        }

        for (const worker of workers) {
            if (typeof worker !== "string" || !worker) {
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
            return new Response(null, {
                status: 200,
                headers: {
                    "Content-Type": "application/x-ndjson"
                }
            });
        }

        // ------------------------------------------------------------
        // Split candidates into batches
        // ------------------------------------------------------------

        const batches = [];

        for (let i = 0; i < candidates.length; i += batchSize) {
            batches.push(
                candidates.slice(i, i + batchSize)
            );
        }

        // ------------------------------------------------------------
        // Output stream
        // ------------------------------------------------------------

        const stream = new TransformStream();
        const writer = stream.writable.getWriter();
        const encoder = new TextEncoder();

        async function writeResult(result) {
            await writer.write(
                encoder.encode(
                    JSON.stringify(result) + "\n"
                )
            );
        }

        // ------------------------------------------------------------
        // Worker pool
        // ------------------------------------------------------------

        const workerCount = Math.min(
            concurrency,
            workers.length,
            batches.length
        );

        let nextBatchIndex = 0;

        async function processWorker(workerUrl) {
            while (true) {
                const batchIndex = nextBatchIndex++;

                if (batchIndex >= batches.length) {
                    return;
                }

                const batch = batches[batchIndex];

                try {
                    const response = await fetch(workerUrl, {
                        method: "POST",
                        headers: {
                            "Content-Type": "application/json"
                        },
                        body: JSON.stringify({
                            candidates: batch,
                            method,
                            headers
                        })
                    });

                    if (!response.ok) {
                        const errorText = await response.text();

                        for (const candidate of batch) {
                            await writeResult({
                                key: candidate.key,
                                url: candidate.url,
                                worker: workerUrl,
                                result: null,
                                error:
                                    `Worker returned HTTP ${response.status}: ${errorText}`
                            });
                        }

                        continue;
                    }

                    let data;

                    try {
                        data = await response.json();
                    } catch {
                        for (const candidate of batch) {
                            await writeResult({
                                key: candidate.key,
                                url: candidate.url,
                                worker: workerUrl,
                                result: null,
                                error: "Worker returned invalid JSON"
                            });
                        }

                        continue;
                    }

                    if (!Array.isArray(data.results)) {
                        for (const candidate of batch) {
                            await writeResult({
                                key: candidate.key,
                                url: candidate.url,
                                worker: workerUrl,
                                result: null,
                                error:
                                    "Worker response missing results array"
                            });
                        }

                        continue;
                    }

                    for (const result of data.results) {
                        await writeResult({
                            key: result.key,
                            url: result.url,
                            worker: workerUrl,
                            result: result.result ?? null
                        });
                    }
                } catch (error) {
                    const message =
                        error instanceof Error
                            ? error.message
                            : String(error);

                    for (const candidate of batch) {
                        await writeResult({
                            key: candidate.key,
                            url: candidate.url,
                            worker: workerUrl,
                            result: null,
                            error: message
                        });
                    }
                }
            }
        }

        // ------------------------------------------------------------
        // Start workers
        // ------------------------------------------------------------

        ctx.waitUntil(
            (async () => {
                try {
                    const tasks = [];

                    for (let i = 0; i < workerCount; i++) {
                        tasks.push(
                            processWorker(workers[i])
                        );
                    }

                    await Promise.all(tasks);
                } finally {
                    await writer.close();
                }
            })()
        );

        // ------------------------------------------------------------
        // Return streaming response
        // ------------------------------------------------------------

        return new Response(stream.readable, {
            status: 200,
            headers: {
                "Content-Type": "application/x-ndjson",
                "Cache-Control": "no-cache"
            }
        });
    }
};