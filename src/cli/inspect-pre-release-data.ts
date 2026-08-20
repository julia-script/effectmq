#!/usr/bin/env node
/** Read-only release gate for pre-v1 EffectMQ keys. */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import { main } from "./InspectPreReleaseData.js";

NodeRuntime.runMain(main);
