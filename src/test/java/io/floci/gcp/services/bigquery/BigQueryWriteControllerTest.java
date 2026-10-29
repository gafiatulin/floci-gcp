package io.floci.gcp.services.bigquery;

import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@QuarkusTest
class BigQueryWriteControllerTest {

    private static final String STREAM = "projects/project/datasets/dataset/tables/table/_default";

    @Test
    void appendRowsCallbacksReturnWhileTheStorageWriteIsBlockedAndKeepRequestOrder() throws Exception {
        BigQueryStorageWrite write = mock(BigQueryStorageWrite.class);
        ExecutorService appendExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch firstAppendStarted = new CountDownLatch(1);
        CountDownLatch allowFirstAppend = new CountDownLatch(1);
        BlockingQueue<String> responses = new LinkedBlockingQueue<>();
        List<String> appended = new ArrayList<>();
        when(write.appendExecutor()).thenReturn(appendExecutor);
        when(write.tryAcquireAppendRequestSlot()).thenReturn(true);
        doAnswer(invocation -> {
            BigQueryStorageWrite.Connection connection = invocation.getArgument(0);
            AppendRowsRequest request = invocation.getArgument(1);
            if (!request.getWriteStream().isEmpty()) {
                connection.writeStream = request.getWriteStream();
            }
            String row = request.getProtoRows().getRows().getSerializedRows(0).toStringUtf8();
            if (row.equals("first")) {
                firstAppendStarted.countDown();
                if (!allowFirstAppend.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting to unblock the first append");
                }
            }
            appended.add(row + ":" + connection.writeStream);
            return AppendRowsResponse.newBuilder().setWriteStream(connection.writeStream).build();
        }).when(write).append(any(), any());

        BigQueryWriteController controller = new BigQueryWriteController(write);
        CountDownLatch completed = new CountDownLatch(1);
        StreamObserver<AppendRowsResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(AppendRowsResponse response) {
                responses.add(response.getWriteStream());
            }

            @Override
            public void onError(Throwable error) {
                throw new AssertionError("AppendRows stream failed", error);
            }

            @Override
            public void onCompleted() {
                completed.countDown();
            }
        };
        StreamObserver<AppendRowsRequest> requests = controller.appendRows(responseObserver);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> first = caller.submit(() -> requests.onNext(request("first", STREAM)));
            assertTrue(firstAppendStarted.await(5, TimeUnit.SECONDS));
            first.get(1, TimeUnit.SECONDS);

            requests.onNext(request("second", ""));
            allowFirstAppend.countDown();

            assertEquals(STREAM, responses.poll(5, TimeUnit.SECONDS));
            assertEquals(STREAM, responses.poll(5, TimeUnit.SECONDS));
            requests.onCompleted();
            assertTrue(completed.await(5, TimeUnit.SECONDS));
            assertEquals(List.of("first:" + STREAM, "second:" + STREAM), appended);
        } finally {
            allowFirstAppend.countDown();
            caller.shutdownNow();
            appendExecutor.shutdownNow();
        }
    }

    private static AppendRowsRequest request(String row, String stream) {
        AppendRowsRequest.Builder request = AppendRowsRequest.newBuilder()
                .setProtoRows(AppendRowsRequest.ProtoData.newBuilder()
                        .setRows(ProtoRows.newBuilder().addSerializedRows(ByteString.copyFromUtf8(row))));
        if (!stream.isEmpty()) {
            request.setWriteStream(stream);
        }
        return request.build();
    }
}
