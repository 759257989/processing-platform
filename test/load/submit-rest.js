// submit-rest.js: 渐进式压 POST /jobs，模拟生产侧 traffic spike。
//
// 阶段（stages）：
//   0  → 1m   : 0 → 100 RPS  warm-up
//   1m → 5m   : 100 → 1000 RPS  ramp-up
//   5m → 10m  : 持续 1000 RPS  steady state
//   10m → 11m : ramp-down 到 0
//
// 输出：default summary + 各阶段 P50/P95/P99/error rate
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const API_URL = __ENV.API_URL || 'http://localhost:8080';

const submitted = new Counter('jobs_submitted');
const failed   = new Rate('jobs_failed_rate');

export const options = {
  scenarios: {
    submit: {
      executor: 'ramping-arrival-rate',   // arrival-rate 风格 = "每秒发 N 个"
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 50,                 // 起手 50 个 VU 复用
      maxVUs: 500,                         // 顶峰最多 500 个 VU
      stages: [
        { target: 100,  duration: '1m' },
        { target: 1000, duration: '4m' },
        { target: 1000, duration: '5m' },
        { target: 0,    duration: '1m' },
      ],
    },
  },
  thresholds: {
    // 验收条件：
    'http_req_duration{tier:realtime}': ['p(99)<2000'],   // realtime P99 < 2s
    'http_req_failed': ['rate<0.01'],                      // 错误率 < 1%
  },
};

// 三种 job type 按 70/20/10 混合
const TYPES = [
  { type: 'REMOTE_COMMAND_EXECUTION', weight: 0.7 },
  { type: 'FIRMWARE_UPDATE_DISPATCH', weight: 0.2 },
  { type: 'TELEMETRY_PROCESSING',     weight: 0.1 },
];

function pickType() {
  const r = Math.random();
  let acc = 0;
  for (const t of TYPES) {
    acc += t.weight;
    if (r < acc) return t.type;
  }
  return TYPES[0].type;
}

export default function () {
  const jobType = pickType();
  const tier = jobType.includes('REMOTE')   ? 'realtime'
              : jobType.includes('FIRMWARE') ? 'bulk'
              : 'standard';

  // device_id 必须跟 devsim seed 命名对齐：`dev-` + 5 位 padding，
  // DB 里 INSERT 的是 dev-00000..dev-00999。dev-5（不 padding）会 FK 违反 → API 500。
  const deviceNum = Math.floor(Math.random() * 1000);
  const deviceID = `dev-${String(deviceNum).padStart(5, '0')}`;

  // idempotency_key 必须 match `^[A-Za-z0-9_-]{8,128}$` — API 校验严格。
  // Math.random() 返回 "0.49281..." 里的小数点违反规则，所以走 base36 字符串。
  const idemSuffix = Math.random().toString(36).slice(2);  // 形如 "m4f8sb2qkj"

  const payload = JSON.stringify({
    type: jobType,
    device_id: deviceID,
    idempotency_key: `${jobType}-${Date.now()}-${idemSuffix}`,
    payload: jobType === 'REMOTE_COMMAND_EXECUTION' ? { command: 'reboot' } :
             jobType === 'FIRMWARE_UPDATE_DISPATCH' ? { target_version: 'v2' } :
                                                      { value: Math.random() * 100 },
  });

  const res = http.post(`${API_URL}/jobs`, payload, {
    headers: { 'Content-Type': 'application/json' },
    tags:    { tier: tier },                          // 给 metric 打 tier label
  });

  const ok = check(res, {
    '202 Accepted': r => r.status === 202,
  });

  submitted.add(1);
  failed.add(!ok);
}