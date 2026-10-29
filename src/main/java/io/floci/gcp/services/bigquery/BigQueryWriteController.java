package io.floci.gcp.services.bigquery;

import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteGrpc;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.FlushRowsRequest;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import io.floci.gcp.core.common.GcpGrpcController;
import io.floci.gcp.core.common.RequestScopes;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

/**
 * gRPC endpoint of the BigQuery Storage Write API. Every call runs with the project of the table
 * it writes to. AppendRows requests are processed serially on bounded workers because table writes can block;
 * responses are sent on the original gRPC context. A rejected append is an in-stream error response, while a
 * malformed connection (no destination) ends the call.
 */
public class BigQueryWriteController extends BigQueryWriteGrpc.BigQueryWriteImplBase {

    private static final int MAX_PENDING_PER_CONNECTION = 64;

    private final BigQueryStorageWrite write;

    public BigQueryWriteController(BigQueryStorageWrite write) {
        this.write = write;
    }

    @Override
    public void createWriteStream(CreateWriteStreamRequest request, StreamObserver<WriteStream> observer) {
        unary(observer, request.getParent(), () -> write.createWriteStream(request));
    }

    @Override
    public void getWriteStream(GetWriteStreamRequest request, StreamObserver<WriteStream> observer) {
        unary(observer, request.getName(), () -> write.getWriteStream(request));
    }

    @Override
    public void finalizeWriteStream(FinalizeWriteStreamRequest request,
                                    StreamObserver<FinalizeWriteStreamResponse> observer) {
        unary(observer, request.getName(), () -> write.finalizeWriteStream(request.getName()));
    }

    @Override
    public void batchCommitWriteStreams(BatchCommitWriteStreamsRequest request,
                                        StreamObserver<BatchCommitWriteStreamsResponse> observer) {
        unary(observer, request.getParent(), () -> write.batchCommitWriteStreams(request));
    }

    @Override
    public void flushRows(FlushRowsRequest request, StreamObserver<FlushRowsResponse> observer) {
        unary(observer, request.getWriteStream(), () -> write.flushRows(request));
    }

    @Override
    public StreamObserver<AppendRowsRequest> appendRows(StreamObserver<AppendRowsResponse> observer) {
        return new AppendRowsObserver(observer, new BigQueryStorageWrite.Connection(), Vertx.currentContext());
    }

    private final class AppendRowsObserver implements StreamObserver<AppendRowsRequest> {
        private final StreamObserver<AppendRowsResponse> observer;
        private final BigQueryStorageWrite.Connection connection;
        private final Context grpcContext;
        private final Deque<AppendRowsRequest> pending = new ArrayDeque<>();

        private boolean closed;
        private boolean processing;
        private boolean completionRequested;

        private AppendRowsObserver(StreamObserver<AppendRowsResponse> observer,
                                   BigQueryStorageWrite.Connection connection, Context grpcContext) {
            this.observer = observer;
            this.connection = connection;
            this.grpcContext = grpcContext;
        }

        @Override
        public void onNext(AppendRowsRequest request) {
            AppendRowsRequest first = null;
            boolean overloaded = false;
            synchronized (this) {
                if (closed || completionRequested) {
                    return;
                }
                if (pending.size() >= MAX_PENDING_PER_CONNECTION || !write.tryAcquireAppendRequestSlot()) {
                    closed = true;
                    releasePendingSlots();
                    overloaded = true;
                } else {
                    pending.addLast(request);
                    if (!processing) {
                        processing = true;
                        first = pending.removeFirst();
                    }
                }
            }
            if (overloaded) {
                onGrpcContext(() -> observer.onError(Status.RESOURCE_EXHAUSTED
                        .withDescription("Too many pending BigQuery Storage Write AppendRows requests")
                        .asRuntimeException()));
            } else if (first != null) {
                submit(first);
            }
        }

        @Override
        public void onError(Throwable t) {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                releasePendingSlots();
            }
        }

        @Override
        public void onCompleted() {
            boolean complete = false;
            synchronized (this) {
                if (closed) {
                    return;
                }
                completionRequested = true;
                if (!processing && pending.isEmpty()) {
                    closed = true;
                    complete = true;
                }
            }
            if (complete) {
                onGrpcContext(observer::onCompleted);
            }
        }

        private void submit(AppendRowsRequest request) {
            try {
                write.appendExecutor().execute(() -> process(request));
            } catch (RejectedExecutionException e) {
                synchronized (this) {
                    closed = true;
                    write.releaseAppendRequestSlot();
                    releasePendingSlots();
                    processing = false;
                }
                onGrpcContext(() -> observer.onError(Status.RESOURCE_EXHAUSTED
                        .withDescription("BigQuery Storage Write AppendRows executor is full")
                        .asRuntimeException()));
            }
        }

        private void process(AppendRowsRequest request) {
            AppendRowsResponse response;
            try {
                response = respond(request);
            } catch (Throwable failure) {
                onGrpcContext(() -> finishRequest(failure));
                return;
            }
            onGrpcContext(() -> {
                Throwable callbackFailure = null;
                try {
                    synchronized (this) {
                        if (!closed) {
                            observer.onNext(response);
                        }
                    }
                } catch (Throwable failure) {
                    callbackFailure = failure;
                }
                finishRequest(callbackFailure);
            });
        }

        private void finishRequest(Throwable failure) {
            write.releaseAppendRequestSlot();
            AppendRowsRequest next = null;
            int abandoned = 0;
            boolean complete = false;
            synchronized (this) {
                if (failure != null) {
                    closed = true;
                }
                if (closed) {
                    abandoned = pending.size();
                    pending.clear();
                    processing = false;
                } else if (!pending.isEmpty()) {
                    next = pending.removeFirst();
                } else {
                    processing = false;
                    if (completionRequested) {
                        closed = true;
                        complete = true;
                    }
                }
            }
            for (int index = 0; index < abandoned; index++) {
                write.releaseAppendRequestSlot();
            }
            if (failure != null) {
                GcpGrpcController.grpcError(observer, failure);
            } else if (next != null) {
                submit(next);
            } else if (complete) {
                observer.onCompleted();
            }
        }

        private void onGrpcContext(Runnable action) {
            // The project-context interceptor keeps request scope on the original Vert.x context for the call.
            if (grpcContext == null || Vertx.currentContext() == grpcContext) {
                action.run();
            } else {
                grpcContext.runOnContext(ignored -> action.run());
            }
        }

        private AppendRowsResponse respond(AppendRowsRequest request) {
            String destination = request.getWriteStream().isEmpty() ? connection.writeStream
                    : request.getWriteStream();
            try {
                return RequestScopes.callAs(
                        destination != null ? BigQueryStorageWrite.projectOf(destination) : null,
                        () -> write.append(connection, request));
            } catch (BigQueryStorageWrite.AppendException e) {
                return e.toResponse(destination);
            }
        }

        private void releasePendingSlots() {
            int queued = pending.size();
            pending.clear();
            for (int index = 0; index < queued; index++) {
                write.releaseAppendRequestSlot();
            }
        }
    }

    private static <T> void unary(StreamObserver<T> observer, String name, Supplier<T> action) {
        try {
            T response = RequestScopes.callAs(BigQueryStorageWrite.projectOf(name), action);
            observer.onNext(response);
            observer.onCompleted();
        } catch (Throwable t) {
            GcpGrpcController.grpcError(observer, t);
        }
    }
}
