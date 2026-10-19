import { util } from '@aws-appsync/utils';

// Channels look like /responses/{sub}/{request_id}. A user may only subscribe
// to their own answers, so one tenant can never read another's stream.
export function onSubscribe(ctx) {
  if (ctx.info.channel.segments[1] !== ctx.identity.sub) {
    util.unauthorized();
  }
}
