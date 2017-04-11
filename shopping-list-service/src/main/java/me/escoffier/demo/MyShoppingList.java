package me.escoffier.demo;

import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.circuitbreaker.CircuitBreaker;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.client.HttpResponse;
import io.vertx.rxjava.ext.web.client.WebClient;
import io.vertx.rxjava.ext.web.codec.BodyCodec;
import io.vertx.rxjava.servicediscovery.ServiceDiscovery;
import io.vertx.rxjava.servicediscovery.types.HttpEndpoint;
import rx.Observable;
import rx.Single;

import static me.escoffier.demo.Shopping.getFallbackPrice;
import static me.escoffier.demo.Shopping.retrievePrice;

/**
 * @author clement
 */
public class MyShoppingList extends AbstractVerticle {

    WebClient shopping, pricer;
    private CircuitBreaker circuit;

    @Override
    public void start() {

        circuit = CircuitBreaker.create("circuit-breaker", vertx,
            new CircuitBreakerOptions()
                .setFallbackOnFailure(true)
                .setMaxFailures(2)
                .setResetTimeout(5000)
                .setTimeout(1000)
        );

        Router router = Router.router(vertx);
        router.route("/health").handler(rc -> rc.response().end("OK"));
        router.route("/").handler(this::getShoppingList);

        ServiceDiscovery.create(vertx, discovery -> {

            // Get pricer-service
            Single<WebClient> pSingle = HttpEndpoint.rxGetWebClient(discovery, rec -> rec.getName().equals("pricer-service"));

            Single<WebClient> sSingle = HttpEndpoint.rxGetWebClient(discovery, rec -> rec.getName().equals("shopping-backend"));

            Single.zip(pSingle, sSingle, (p, s) -> {
                pricer = p;
                shopping = s;

                return vertx.createHttpServer()
                    .requestHandler(router::accept)
                    .listen(8080);
            }).subscribe();

            // Get shopping-backend
            

        });
    }

    private void getShoppingList(RoutingContext rc) {
        HttpServerResponse serverResponse =
            rc.response().setChunked(true);

        Single<JsonObject> single = shopping.get("/shopping")
            .as(BodyCodec.jsonObject())
            .rxSend()
            .map(HttpResponse::body);

        single.subscribe(
            list -> {
                Observable.from(list)
                    .flatMap(entry ->
                        circuit.rxExecuteCommandWithFallback(
                            future -> retrievePrice(pricer, entry, future),
                            t -> getFallbackPrice(entry)
                        )
                            .toObservable()
                    )
                    .subscribe(
                        res -> Shopping.writeProductLine(serverResponse, res),
                        rc::fail,
                        serverResponse::end
                    );
            }
        );

    }

}
