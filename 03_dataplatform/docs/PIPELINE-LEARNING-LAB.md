# Pipeline Learning Lab 변경 사항

## 포함 파일
- `api/main.py`: 관리자 전용 `POST /learning/reset` 추가
- `frontend/app/pipeline-lab/page.tsx`: 랜덤 이벤트 생성, Pipeline/Kafka 모니터링, Iceberg snapshot/time-travel 단일 UI
- `frontend/app/page.tsx`: 사이드바에 Pipeline Learning Lab 메뉴 추가
- `frontend/app/globals.css`: Lab 버튼 및 필터 UI 스타일 추가

## 적용
1. 현재 `api/main.py`와 `frontend/`를 백업합니다.
2. ZIP 내부 `api/main.py`를 프로젝트 `api/main.py`에 반영합니다.
3. ZIP 내부 `frontend/` 파일을 프로젝트 `frontend/`에 반영합니다.
4. 프로젝트 루트에서 실행합니다.

```powershell
docker compose up -d --build api frontend
```

브라우저에서 기존 메인 화면에 로그인한 다음 `/pipeline-lab`으로 이동합니다. 시작 버튼은 기존 `POST /usage` API를 주기적으로 호출하므로 Kafka/Spark 파이프라인이 실행 중이어야 Iceberg 데이터가 추가됩니다.

## 초기화 범위 및 주의
`POST /learning/reset`은 `ldap-admin`, `data-admin`, `platform-admin`, `realm-admin` 역할만 호출할 수 있습니다. 현재 API 프로세스의 파이프라인 상태와 Iceberg 테이블의 현재 행을 초기화합니다. Iceberg `DELETE`는 새 스냅샷을 만들기 때문에 과거 Time Travel 이력은 유지됩니다.

Kafka 토픽 로그는 실행 중인 Spark Structured Streaming의 토픽/파티션 체크포인트를 손상시키지 않도록 보존합니다. 따라서 UI의 초기화 확인 문구에도 Kafka 로그 보존 사실을 명시했습니다. 브로커 로그까지 완전히 비우는 운영 초기화는 토픽 삭제/재생성 및 Spark checkpoint 재초기화 절차를 별도로 설계한 뒤 수행해야 하며, 이 엔드포인트에서는 수행하지 않습니다.
