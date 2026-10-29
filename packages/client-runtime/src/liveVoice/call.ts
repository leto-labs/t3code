export interface LiveVoiceTrack {
  enabled: boolean;
  readonly stop: () => void;
}

export interface LiveVoiceMedia<Stream> {
  readonly tracks: ReadonlyArray<LiveVoiceTrack>;
  readonly stream: Stream;
}

export interface LiveVoicePeer<Stream> {
  readonly addMedia: (stream: Stream) => void;
  readonly createOffer: () => Promise<{ readonly sdp?: string | null | undefined }>;
  readonly setLocalDescription: (offer: {
    readonly sdp?: string | null | undefined;
  }) => Promise<void>;
  /** Resolves once the local description includes gathered ICE candidates. */
  readonly waitForIceGathering: () => Promise<void>;
  readonly localDescriptionSdp: () => string | undefined;
  readonly setRemoteDescription: (answer: {
    readonly type: "answer";
    readonly sdp: string;
  }) => Promise<void>;
  /** Queued until the provider's data channel opens. */
  readonly sendRealtimeEvent: (message: string) => void;
  /** Silences the speaker and records remote audio; false when it cannot be replayed. */
  readonly hold: () => boolean;
  /** Plays what was recorded on hold, then the live stream. */
  readonly resume: () => Promise<void>;
  readonly close: () => void;
}

export interface LiveVoiceCallDependencies<Stream> {
  readonly acquireMedia: () => Promise<LiveVoiceMedia<Stream>>;
  /** Runs while the microphone and ICE are set up, to hide the provider's startup. */
  readonly prepare: () => Promise<void>;
  readonly createPeer: () => LiveVoicePeer<Stream>;
  readonly start: (
    offerSdp: string,
  ) => Promise<{ readonly callId: string; readonly answerSdp: string }>;
  readonly stop: () => Promise<void>;
}

/** One WebRTC negotiation and teardown; the registry decides when to run it. */
export class LiveVoiceCall<Stream> {
  private media: LiveVoiceMedia<Stream> | null = null;
  private peer: LiveVoicePeer<Stream> | null = null;
  private starting: Promise<string> | null = null;
  private stopping: Promise<void> | null = null;
  private closed = false;
  private held = false;
  private readonly dependencies: LiveVoiceCallDependencies<Stream>;

  constructor(dependencies: LiveVoiceCallDependencies<Stream>) {
    this.dependencies = dependencies;
  }

  get mediaStream(): Stream | undefined {
    return this.media?.stream;
  }

  /** Resolves with the server's call id once the answer is applied. */
  start(): Promise<string> {
    this.starting ??= this.negotiate();
    return this.starting;
  }

  private async negotiate(): Promise<string> {
    const prepared = this.dependencies.prepare();
    try {
      const media = await this.dependencies.acquireMedia();
      this.media = media;
      this.applyMicrophone();
      await prepared;
      if (this.closed) throw new Error("Live voice was ended.");
      const peer = this.dependencies.createPeer();
      this.peer = peer;
      peer.addMedia(media.stream);
      if (this.held) peer.hold();
      const offer = await peer.createOffer();
      await peer.setLocalDescription(offer);
      await peer.waitForIceGathering();
      // A remote answerer needs the gathered candidates, which only the local description has.
      const sdp = peer.localDescriptionSdp() ?? offer.sdp;
      if (!sdp) throw new Error("The browser did not produce a WebRTC offer.");
      // The provider's SDP parser fails with "EOF" without the final line terminator.
      const offerSdp = sdp.endsWith("\r\n") ? sdp : `${sdp}\r\n`;
      if (this.closed) throw new Error("Live voice was ended.");
      const { callId, answerSdp } = await this.dependencies.start(offerSdp);
      if (this.closed) throw new Error("Live voice was ended.");
      await peer.setRemoteDescription({ type: "answer", sdp: answerSdp });
      return callId;
    } catch (error) {
      await prepared.catch(() => undefined);
      await this.stop();
      throw error;
    }
  }

  private applyMicrophone(): void {
    for (const track of this.media?.tracks ?? []) track.enabled = !this.held;
  }

  /** Mutes both directions while the transport stays up; returns whether audio is buffered. */
  hold(): boolean {
    this.held = true;
    this.applyMicrophone();
    return this.peer?.hold() ?? false;
  }

  async resume(): Promise<void> {
    if (!this.held) return;
    this.held = false;
    await this.peer?.resume();
    if (!this.held) this.applyMicrophone();
  }

  sendRealtimeEvent(message: string): void {
    this.peer?.sendRealtimeEvent(message);
  }

  /** Ends the call locally and on the server. Safe to call repeatedly. */
  stop(): Promise<void> {
    this.stopping ??= (async () => {
      this.close();
      // prepare() may have opened a provider thread even before start was sent.
      await this.dependencies.stop().catch(() => undefined);
    })();
    return this.stopping;
  }

  /** Releases media and the peer; the server already ended the call. */
  close(): void {
    this.closed = true;
    this.peer?.close();
    for (const track of this.media?.tracks ?? []) track.stop();
    this.peer = null;
    this.media = null;
  }
}
