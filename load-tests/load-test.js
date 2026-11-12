// Usage:
//   k6 run load-tests/load-test.js                    -> 1000 users, each picks a random seat
//   k6 run -e MODE=hot load-tests/load-test.js        -> everyone fights for seat A1 (exactly ONE must win)
//   k6 run -e VUS=3000 load-tests/load-test.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE || 'http://localhost:8080';
const MODE = __ENV.MODE || 'spread';
const VUS = parseInt(__ENV.VUS || '1000');

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 409, 429));

const booked = new Counter('seats_booked_201');
const conflicts = new Counter('seat_conflicts_409');

export const options = {
  scenarios: {
    rush: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: VUS },
        { duration: '60s', target: VUS },
        { duration: '10s', target: 0 },
      ],
    },
  },
  thresholds: { http_req_failed: ['rate<0.05'], http_req_duration: ['p(95)<2000'] },
};

export default function () {
  const user = `u-${__VU}-${__ITER}`;
  const h = { headers: { 'X-User-Id': user, 'Content-Type': 'application/json' } };

  http.post(`${BASE}/api/queue/join`, null, h);

  let token = null;
  for (let i = 0; i < 120 && !token; i++) {
    const r = http.get(`${BASE}/api/queue/status`, h);
    if (r.status === 200) {
      const s = r.json();
      if (s.status === 'ADMITTED') token = s.accessToken;
    }
    if (!token) sleep(1);
  }
  if (!token) return;

  const seat = MODE === 'hot'
    ? 'A1'
    : String.fromCharCode(65 + Math.floor(Math.random() * 10)) + (1 + Math.floor(Math.random() * 100));

  const res = http.post(`${BASE}/api/bookings`,
    JSON.stringify({ eventId: 1, seatNumber: seat }),
    { headers: { 'X-User-Id': user, 'X-Access-Token': token, 'Content-Type': 'application/json' } });

  check(res, { '201 or 409': (r) => r.status === 201 || r.status === 409 });
  if (res.status === 201) booked.add(1);
  if (res.status === 409) conflicts.add(1);
  sleep(1);
}
