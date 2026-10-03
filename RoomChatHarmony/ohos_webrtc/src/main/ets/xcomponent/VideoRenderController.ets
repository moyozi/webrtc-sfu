/**
 * Copyright (c) 2024 Archermind Technology (Nanjing) Co. Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { NativeVideoRenderer, MediaStreamTrack } from 'libohos_webrtc.so';
import { Logging } from '../log/Logging'

const TAG: string = '[VideoRenderController]';

export enum ScalingMode { Fill, AspectFill, AspectFit }

export class VideoRenderController extends XComponentController {
  private renderer: NativeVideoRenderer = new NativeVideoRenderer();

  // 记录最近一次设置, 供 surface 创建后补挂。
  // 原生 renderer 只有在 init(surfaceId) 之后才存在; 若在此之前调用
  // setVideoTrack, AddSink 会因为 renderer_ 为空而直接返回, sink 不会被挂上,
  // 导致先设置轨道、后创建 XComponent 的画面不出来。
  private pendingTrack: MediaStreamTrack | null = null;
  private mirror: boolean = false;
  private mirrorVertically: boolean = false;
  private scalingMode: ScalingMode = ScalingMode.Fill;

  setVideoTrack(track: MediaStreamTrack | null): void {
    this.pendingTrack = track;
    this.renderer.setVideoTrack(track);
  }

  setMirror(mirror: boolean) : void {
    this.mirror = mirror;
    this.renderer.setMirror(mirror);
  }

  setMirrorVertically(mirrorVertically: boolean): void {
    this.mirrorVertically = mirrorVertically;
    this.renderer.setMirrorVertically(mirrorVertically);
  }

  setScalingMode(mode: ScalingMode) : void {
    this.scalingMode = mode;
    this.renderer.setScalingMode(mode);
  }

  onSurfaceCreated(surfaceId: string): void {
    Logging.d(TAG, 'onSurfaceCreated surfaceId: ' + surfaceId);
    this.renderer.init(surfaceId);

    // init 之后 renderer 才真正创建, 补上之前被忽略的镜像/缩放/轨道设置。
    this.renderer.setMirror(this.mirror);
    this.renderer.setMirrorVertically(this.mirrorVertically);
    this.renderer.setScalingMode(this.scalingMode);
    if (this.pendingTrack != null) {
      this.renderer.setVideoTrack(this.pendingTrack);
    }
  }

  onSurfaceChanged(surfaceId: string, rect: SurfaceRect): void
  {
    Logging.d(TAG, 'onSurfaceChanged surfaceId: ' + surfaceId);
    Logging.d(TAG, 'onSurfaceChanged rect: ' + rect);
  }

  onSurfaceDestroyed(surfaceId: string): void
  {
    Logging.d(TAG, 'onSurfaceDestroyed surfaceId: ' + surfaceId);
    this.renderer.release();
  }
}
