# RabbitMQ 기능 패리티 조사 (dedup / idempotency / serving revision)

2026-07-31 기준. RedisBroker 전용 기능들을 순수 RabbitMQ(플러그인 포함, 외부 KV 없음)로
재현할 수 있는지 조사한 결과와 그 근거를 기록한다.

## 요약

| 기능 | 순수 RabbitMQ | 판정 근거 |
| --- | --- | --- |
| dedup — simple (대기 중 거부, ack 시 해제) | **플러그인으로 가능** | rabbitmq-message-deduplication 큐 레벨 dedup이 동일 시맨틱 (ack/drop 시 캐시 해제, 소스 확인) |
| dedup — throttle (TTL 윈도우 거부) | **플러그인으로 가능** | exchange 레벨 dedup + `x-cache-ttl` (건별 override 가능) |
| dedup — debounce+replace (대기 페이로드 교체) | **불가** | 큐에 들어간 메시지를 교체/선택 삭제하는 프리미티브가 브로커에 없음. 플러그인도 "거부"만 가능 |
| idempotency (begin/heartbeat/commit, fence token) | **불가** | fence token을 발급할 프리미티브 부재. Jepsen이 RabbitMQ 분산 락의 이중 보유를 실증 |
| serving revision (CAS 승격 + TTL presence) | **패리티 불가** | CAS/조건부 갱신 API 부재 (Khepri는 사용자 비노출). 근사 재설계만 가능 |

## dedup 상세

- 커뮤니티 플러그인 [rabbitmq-message-deduplication](https://github.com/noxdafox/rabbitmq-message-deduplication)은
  2026-07 릴리스(0.8.0)까지 활발히 유지보수되며 RabbitMQ 4.0.x용 공식 빌드를 제공한다.
- 큐 레벨 dedup(`x-message-deduplication: true`)은 fluxera의 simple 모드와 정확히 일치:
  같은 `x-deduplication-header`가 큐에 있으면 거부, ack/drop 시 해제.
- 채택 시 리스크: **classic queue 한정**(quorum 미지원 — `rabbit_backing_queue` 전역 치환
  구현), 단일 메인테이너, 4.3 업그레이드 시 플러그인 임시 비활성화 필요, 캐시 풀 시 랜덤
  evict로 TTL 윈도우가 best-effort로 약화, 거부된 발행에 대한 퍼블리셔 피드백 없음,
  성능 수치 미공개. 큐 초기화 시 캐시 flush로 재시작 직후 일시적 중복 허용 가능(소스 판독
  기반 추론).
- 네이티브 기능은 없음: AMQP 1.0 message-id 기반 브로커 dedup은 미구현
  ([rabbitmq-server#3843](https://github.com/rabbitmq/rabbitmq-server/issues/3843) open),
  Streams publisher dedup은 단조 증가 publishing id 기반이라 비즈니스 id 거부 용도가 아님.

## idempotency 상세

순수 RabbitMQ로 fence token 있는 keyed 락+결과 저장을 만들 수 없다:

- exclusive queue 락은 커넥션 수명에 종속되고 fence token이 없다.
  [Jepsen: RabbitMQ](https://aphyr.com/posts/315-jepsen-rabbitmq)는 파티션 시 두 클라이언트가
  동시에 락을 보유함을 실증했다 ("It's just not the right fit for a lock service").
- Streams는 compaction/TTL/키 조회가 없어 KV 대체가 안 되고, MQTT retained store는
  노드 로컬이라 클러스터에서 신뢰 불가
  ([rabbitmq-server#8096](https://github.com/rabbitmq/rabbitmq-server/issues/8096) open).
- Single Active Consumer + consistent-hash exchange로 키 직렬화(락 필요성 축소)는 가능하나
  AMQP 0-9-1은 active 여부 통지가 없고 펜싱이 없으며, 결과 저장소 시맨틱이 아예 없다.

## serving revision 상세

- AMQP 프로토콜/management API에 CAS(조건부 갱신)가 없고, 메타데이터 저장소(Khepri)는
  사용자에게 노출되지 않는다.
- 근사 재설계는 가능: presence는 exclusive 큐 존재로(감지 지연 큼, 메타데이터 불가),
  라우팅 전환은 리비전별 큐 rebind(비원자적 — 유실 또는 이중 라우팅 창 존재),
  현재 리비전 값은 스트림 이벤트 소싱(선형화 CAS가 아닌 최종 수렴). 실무 사례들은 전환
  결정을 RabbitMQ 밖(LB/배포 도구)에서 내린다.

## LavinMQ의 경우

LavinMQ(AMQP 0-9-1 호환, Crystal 구현)는 전송 계층으로 검증 완료: `RabbitMQBroker`가
수정 없이 동작한다 (통합 테스트 38개, 복원력 시나리오 3종, 벤치마크 전부 통과 —
2026-07-31, LavinMQ 2.x). 이 문서의 결론에 미치는 영향:

- **dedup**: 플러그인 시스템은 없지만 dedup이 내장(2.2.0+, RabbitMQ 플러그인과 같은
  인자 규약). 실측 결과 TTL 윈도우(throttle) 시맨틱은 동작하나, **ack 시 키 해제
  (simple 모드)가 없고** 캐시가 ack 후에도 유지된다. debounce+replace는 불가.
- **idempotency / serving revision**: KV/CAS 부재는 RabbitMQ와 동일 (etcd는 내부
  클러스터 조정 전용으로 미노출). 결론 변화 없음.
- 운영상 이점: consumer timeout 기본 비활성(장시간 태스크에 유리, `x-consumer-timeout`
  설정 시 ~60초 주기로 강제 — 실측 27초 내 재전달), management API 통계 실시간,
  delayed message exchange 내장. 단 quorum queue는 조용히 classic으로 대체된다.

## 결론과 권장

RabbitMQ는 at-least-once 전달을 위해 재전달하도록 설계됐고, 그 성질이 락/CAS 같은 조정
프리미티브와 충돌한다는 것이 문헌의 일관된 결론이다. 따라서:

1. dedup만 필요하면 — dedup 플러그인 채택을 검토할 수 있다 (simple/throttle 한정,
   위 리스크 수용 시). RabbitMQBroker에 플러그인 연동 옵션을 추가하는 것은 실현 가능한
   후속 작업이다.
2. idempotency/revision까지 필요하면 — 전송은 RabbitMQ, 조정 상태는 Redis를 쓰는
   하이브리드가 정석이다. 순수 RabbitMQ 경로는 기능 패리티가 아니라 시맨틱 하향을 수반하는
   재설계가 된다.

출처 전체 목록은 조사 원문(2026-07-31 세션) 참조. 핵심 출처:
[RabbitMQ Queues](https://www.rabbitmq.com/docs/queues) ·
[RabbitMQ Reliability](https://www.rabbitmq.com/docs/reliability) ·
[RabbitMQ Streams](https://www.rabbitmq.com/docs/streams) ·
[Metadata store](https://www.rabbitmq.com/docs/metadata-store) ·
[Jepsen: RabbitMQ](https://aphyr.com/posts/315-jepsen-rabbitmq) ·
[Kleppmann: How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) ·
[rabbitmq-message-deduplication](https://github.com/noxdafox/rabbitmq-message-deduplication)
