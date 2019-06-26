package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/txn2/provision"

	"github.com/txn2/micro"
	"github.com/txn2/qlrx"
	"go.uber.org/zap"
)

var (
	tcpIpEnv            = getEnv("TCP_IP", "127.0.0.1")
	tcpPortEnv          = getEnv("TCP_PORT", "3000")
	tcpReadTimeoutEnv   = getEnv("TCP_READ_TIMEOUT", "10")
	tcpBufferSizeEnv    = getEnv("TCP_BUFFER_SIZE", "1600")
	provisionServiceEnv = getEnv("PROVISION_SERVICE", "http://api-provision:8070")
	modelServiceEnv     = getEnv("MODEL_SERVICE", "http://api-tm:8070")
	ingestTsServiceEnv  = getEnv("INGEST_TS_SERVICE", "http://rxtx-ts:80")
	ingestIdServiceEnv  = getEnv("INGEST_ID_SERVICE", "http://rxtx-id:80")
	assetIdPrefixEnv    = getEnv("ASSET_ID_PREFIX", "imei-")
	systemPrefixEnv     = getEnv("SYSTEM_PREFIX", "system_")
)

// MsgResp is a response to messages
type MsgResp struct {
	Heartbeat bool
	Protocol  string
	Count     string
	Type      string
	Id        string
}

func main() {
	tcpReadTimeoutEnvInt, err := strconv.Atoi(tcpReadTimeoutEnv)
	if err != nil {
		fmt.Printf("TCP listener read timeout must be integer, parse error: " + err.Error())
	}

	tcpBufferSizeEnvInt, err := strconv.Atoi(tcpBufferSizeEnv)
	if err != nil {
		fmt.Printf("TCP buffer size must be an integer, parse error: " + err.Error())
	}

	var (
		tcpIp            = flag.String("tcpIp", tcpIpEnv, "TCP listener IP address.")
		tcpPort          = flag.String("tcpPort", tcpPortEnv, "TCP listener port.")
		tcpReadTimeout   = flag.Int("tcpReadTimeout", tcpReadTimeoutEnvInt, "TCP listener read timeout.")
		tcpBuffer        = flag.Int("tcpBufferSize", tcpBufferSizeEnvInt, "TCP buffer size in bytes.")
		assetIdPrefix    = flag.String("assetIdPrefix", assetIdPrefixEnv, "Asset ID prefix.")
		systemPrefix     = flag.String("systemPrefix", systemPrefixEnv, "Prefix for system indices.")
		provisionService = flag.String("provisionService", provisionServiceEnv, "Provisioning service.")
		modelService     = flag.String("modelService", modelServiceEnv, "Model service.")
		ingestTsService  = flag.String("ingestTsService", ingestTsServiceEnv, "Ingest time-series message service.")
		ingestIdService  = flag.String("ingestIdService", ingestIdServiceEnv, "Ingest id-based message service.")
	)

	serverCfg, _ := micro.NewServerCfg("qlrx")
	server := micro.NewServer(serverCfg)

	// background web server
	go func() {
		server.Run()
	}()

	qlApi, err := qlrx.NewApi(&qlrx.Config{
		Logger:     server.Logger,
		HttpClient: server.Client,
	})

	conditionParser := func(msg *string, elms *[]string, conditions []provision.ConditionCfg) bool {
		for _, cond := range conditions {

			// message regex
			if cond.Parser == "qlrx_msg_regex" {
				re, err := regexp.Compile(cond.Condition)
				if err != nil {
					server.Logger.Error("Could not compile regex for qlrx_msg_regex.",
						zap.Error(err),
						zap.String("condition", cond.Condition),
					)
					return false
				}

				if re.MatchString(*msg) == false {
					return false
				}

				continue
			}

			// index regex
			if cond.Parser == "qlrx_idx_regex" {
				// first split the condition
				condElms := strings.Split(cond.Condition, "|")
				if len(condElms) != 2 {
					server.Logger.Error("qlrx_idx_regex has the wrong number of elements, should be 2 (idx|regex)",
						zap.String("condition", cond.Condition),
						zap.Strings("condition_elms", condElms),
					)
					return false
				}

				// convert second elm to int
				idx, err := strconv.Atoi(condElms[1])
				if err != nil {
					server.Logger.Error("can not convert qlrx_idx_regex idx to int. (should be idx|regex)",
						zap.String("condition", cond.Condition),
						zap.Strings("condition_elms", condElms),
					)
					return false
				}

				// is index in range?
				if idx > len(*elms) {
					server.Logger.Error("qlrx_idx_regex - index is greater than the number of elements in the message.",
						zap.String("condition", cond.Condition),
						zap.Strings("condition_elms", condElms),
					)
					return false
				}

				re, err := regexp.Compile(condElms[1])
				if err != nil {
					server.Logger.Error("Could not compile regex for qlrx_idx_regex.", zap.Error(err),
						zap.String("condition", cond.Condition),
						zap.Strings("condition_elms", condElms),
					)
					return false
				}

				if re.MatchString(*msg) == false {
					return false
				}

				continue
			}

			// element greater than
			if cond.Parser == "qlrx_idx_count_gt" || cond.Parser == "qlrx_idx_count_lt" {
				// convert second elm to int
				idx, err := strconv.Atoi(cond.Condition)
				if err != nil {
					server.Logger.Error("condition must be in integer for qlrx_idx_count_gt and qlrx_idx_count_lt.",
						zap.String("condition", cond.Condition),
					)
					return false
				}

				if cond.Parser == "qlrx_idx_count_gte" && len(*elms) <= idx {
					return false
				}

				if cond.Parser == "qlrx_idx_count_lte" && len(*elms) >= idx {
					return false
				}

			}

		}

		// if no valid parser conditions returned false
		return true
	}

	// Handle TCP connection
	tcpHandler := func(c net.Conn) {
		defer c.Close()

		server.Logger.Info("Serving", zap.String("remote", c.RemoteAddr().String()))
		buf := make([]byte, *tcpBuffer)

		bufLen, err := c.Read(buf)
		if err != nil {
			if err != io.EOF {
				server.Logger.Error("TCP Read Error", zap.Error(err))
			}
			return
		}

		msgData := buf[:bufLen]
		server.Logger.Info("Messages received", zap.ByteString("message", msgData))

		// message must end with $
		msgStr := strings.TrimSpace(string(msgData))
		if msgStr[len(msgStr)-1:] != "$" {
			server.Logger.Warn("message did not terminate with a $")
			return
		}

		// may receive multiple messages
		msgs := strings.Split(msgStr, "$")
		msgResp := make([]MsgResp, len(msgs)-1)

		// parse each message
		for i, msg := range msgs {
			// split each message
			elms := strings.Split(msg, ",")
			if len(elms) < 3 {
				continue
			}

			// index 0 = message type
			msgType := strings.Split(elms[0], ":")
			if len(msgType) != 2 {
				server.Logger.Warn("Unknown message type", zap.String("type", elms[0]))
				continue
			}

			// index 1 = protocol version
			msgResp[i].Type = msgType[1]
			msgResp[i].Protocol = elms[1]
			msgResp[i].Id = elms[2]
			msgResp[i].Count = elms[len(elms)-1]

			server.Logger.Debug("Lookup",
				zap.Int("elms", len(elms)),
				zap.String("id", msgResp[i].Id),
				zap.String("type", msgResp[i].Type),
				zap.String("protocol", msgResp[i].Protocol),
				zap.String("count", msgResp[i].Count),
			)

			// Get asset
			asset, err := qlApi.GetAsset(*provisionService, *assetIdPrefix, msgResp[i].Id)
			if err != nil {
				server.Logger.Warn("Unable to retrieve asset related to message.", zap.Error(err))
				continue
			}

			// ROUTES for every account / model assignment in asset
			for _, a := range asset.Routes {

				// if the route has conditions check them here and bail if any are
				// false.
				if conditionParser(&msg, &elms, a.Conditions) != true {
					server.Logger.Debug("Message did not meet route condition.")
					continue
				}

				modelId := a.ModelId

				server.Logger.Debug("Route message for asset",
					zap.String("account", a.AccountId),
					zap.String("base_model", a.ModelId),
					zap.String("type", a.Type),
					zap.String("model", modelId),
				)

				modelPrefix := a.AccountId
				if a.Type == "system" {
					modelPrefix = *systemPrefix
				}

				// Get model
				// [base_model]_[MSG_TYPE]_[PROTOCOL]
				model, err := qlApi.GetModel(*modelService, modelPrefix, modelId)
				if err != nil {
					server.Logger.Warn("Unable to retrieve model related to asset.", zap.Error(err))
					continue
				}

				// convert elms to model-based payload
				payloadJson := qlApi.Package(elms, model)

				// record stored by the id and by time-series
				for _, service := range []string{*ingestTsService, *ingestIdService} {
					url := fmt.Sprintf(
						"%s/rx/%s/%s/%s/device",
						service,
						a.AccountId,
						model.MachineName,
						msgResp[i].Id,
					)
					err = qlApi.Inject(url, payloadJson)
					if err != nil {
						server.Logger.Warn("Could not inject payload", zap.String("url", url), zap.Error(err))
					}
					server.Logger.Info("Injected Message", zap.String("url", url))
					server.Logger.Debug("Payload", zap.ByteString("payload", payloadJson))
				}
			}

			if msgResp[i].Count != "" {
				sendAck := fmt.Sprintf("+SACK:%s$", msgResp[i].Count)
				server.Logger.Debug("TCP Write", zap.String("ACK", sendAck))
				_, err = c.Write([]byte(sendAck))
				if err != nil {
					server.Logger.Error("TCP Write Error", zap.Error(err))
				}
			}

			// TODO RETURN queued commands
		}
	}

	// Listen for TCP connections
	l, err := net.Listen("tcp4", fmt.Sprintf("%s:%s", *tcpIp, *tcpPort))
	if err != nil {
		fmt.Println(err)
		return
	}

	server.Logger.Info("TCP server started",
		zap.String("ip", *tcpIp),
		zap.String("port", *tcpPort),
		zap.Int("buffer_size", *tcpBuffer),
	)

	// for each connection
	for {
		c, err := l.Accept()
		if err != nil {
			server.Logger.Error("TCP Accept Error", zap.Error(err))
			return
		}

		err = c.SetDeadline(time.Now().Add(time.Duration(*tcpReadTimeout) * time.Second))
		if err != nil {
			server.Logger.Error("TCP SetDeadline Error", zap.Error(err))
			return
		}

		go tcpHandler(c)
	}

}

// getEnv gets an environment variable or sets a default if
// one does not exist.
func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}

	return value
}
