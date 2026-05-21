// read-rest.js: 100 RPS 持续打 GET /jobs/<known-uuid>
// 用来验证读路径在写压力下还能保持低延迟。
import http from 'k6/http';
import { check } from 'k6';

const API_URL = __ENV.API_URL || 'http://localhost:8080';
// 预先准备好一个 job id 写在 env 里；可以用 submit-rest 跑一会儿、随便挑一个
const SAMPLE_JOB_ID = __ENV.SAMPLE_JOB_ID;

export const options = {
  scenarios: {
    read: {
      executor: 'constant-arrival-rate',
      rate: 100,
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 30,
      maxVUs: 100,
    },
  },
  thresholds: {
    'http_req_duration': ['p(99)<200'],   // GET 比 POST 快得多
    'http_req_failed':   ['rate<0.01'],
  },
};

export default function () {
  if (!SAMPLE_JOB_ID) throw new Error("set SAMPLE_JOB_ID env var");
  const res = http.get(`${API_URL}/jobs/${SAMPLE_JOB_ID}`);
  check(res, { '200 OK': r => r.status === 200 });
}